import os
import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

# ユーザー提供のモジュール
from selenium_helper.selenium_helper import SeleniumBrowser
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# .env ファイルを読み込む (ファイルがない場合は環境変数を参照)
load_dotenv()

# --- 1. 設定管理クラス ---
@dataclass(frozen=True)
class Config:
    """
    設定情報を保持するデータクラス
    .env ファイルまたは環境変数から値を読み込みます。
    """
    # Zaim認証情報
    ZAIM_EMAIL: str = os.getenv("ZAIM_EMAIL", "")
    ZAIM_PASS: str = os.getenv("ZAIM_PASS", "")
    
    # Google Sheets設定
    SPREADSHEET_KEY: str = os.getenv("SPREADSHEET_KEY", "")
    JSON_KEYFILE: str = os.getenv("JSON_KEYFILE", "service_account.json")
    
    # URL設定 (基本変更不要なためハードコードのまま)
    URL_LOGIN: str = "https://auth.zaim.net/"
    URL_HISTORY: str = "https://content.zaim.net/money"
    
    # Firefox & Selenium Helper 設定
    GECKODRIVER_PATH: str = os.getenv("GECKODRIVER_PATH", "./geckodriver")
    FIREFOX_BINARY_PATH: str = os.getenv("FIREFOX_BINARY_PATH", "")
    FIREFOX_PROFILE_PATH: str = os.getenv("FIREFOX_PROFILE_PATH", "")

    def validate(self):
        """必須設定が読み込まれているかチェック"""
        missing = []
        if not self.ZAIM_EMAIL: missing.append("ZAIM_EMAIL")
        if not self.ZAIM_PASS: missing.append("ZAIM_PASS")
        if not self.SPREADSHEET_KEY: missing.append("SPREADSHEET_KEY")
        if not self.FIREFOX_BINARY_PATH: missing.append("FIREFOX_BINARY_PATH")
        if not self.FIREFOX_PROFILE_PATH: missing.append("FIREFOX_PROFILE_PATH")
        
        if missing:
            raise ValueError(f"以下の環境変数が設定されていません: {', '.join(missing)}\n.envファイルを確認してください。")

# --- 2. ブラウザ管理クラス (selenium_helper ラッパー) ---
class BrowserManager:
    """
    selenium_helper.SeleniumBrowser のライフサイクルを管理する
    Context Manager (with構文対応)
    """
    def __init__(self, config: Config, headless: bool = True):
        self.config = config
        self.headless = headless
        self.helper_browser: Optional[SeleniumBrowser] = None

    def __enter__(self) -> SeleniumBrowser:
        """with構文開始時にブラウザを起動"""
        print(">> Firefoxを起動中...")
        
        # selenium_helper の仕様に合わせた設定辞書を作成
        browser_setting = {
            "browser_path": self.config.FIREFOX_BINARY_PATH,
            "browser_profile": self.config.FIREFOX_PROFILE_PATH
        }
        
        self.helper_browser = SeleniumBrowser(
            geckodriver_path=self.config.GECKODRIVER_PATH,
            headless=self.headless,
            browser_setting=browser_setting,
            # set_size=True # ウィンドウサイズを固定
        )
        return self.helper_browser

    def __exit__(self, exc_type, exc_value, traceback):
        """with構文終了時にブラウザを閉じる"""
        if self.helper_browser:
            print(">> Firefoxを終了します")
            self.helper_browser.close_selenium()

# --- 3. Zaimスクレイピングクラス ---
class ZaimScraper:
    """Zaim固有の操作を担当 (selenium_helper利用)"""
    def __init__(self, helper: SeleniumBrowser, config: Config):
        self.helper = helper
        self.driver = helper.browser # selenium_helper内の生のdriverオブジェクト
        self.config = config
        self.wait = WebDriverWait(self.driver, 15)

    def login(self):
        """Zaimへのログイン処理"""
        print(">> Zaimへログインを開始します...")
        
        # helperのリトライ付きGETを使用
        self.helper.recur_selenium_get(self.config.URL_LOGIN)
        
        # 要素待機と入力
        try:
            email_input = self.wait.until(EC.visibility_of_element_located((By.ID, "email")))
            email_input.send_keys(self.config.ZAIM_EMAIL)
            
            self.driver.find_element(By.ID, "password").send_keys(self.config.ZAIM_PASS)
            self.driver.find_element(By.NAME, "action").click()
            
            time.sleep(5) # 遷移待機
        except Exception as e:
            print(f"ログイン中にエラーが発生しました: {e}")
            raise

    def fetch_history_data(self) -> Optional[pd.DataFrame]:
        """履歴ページからデータを取得"""
        print(">> 履歴データを取得中...")
        
        self.helper.recur_selenium_get(self.config.URL_HISTORY)
        
        # 必要に応じてスクロール
        # self.helper.recur_scroll_down(speed=500)
        
        try:
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        except:
            print("テーブルが見つかりません。ログイン失敗か、データが存在しません。")
            return None
        
        html = self.driver.page_source
        dfs = pd.read_html(html)
        
        if not dfs:
            return None
            
        return dfs[0]

# --- 4. データ加工クラス ---
class DataProcessor:
    """データの整形ロジック"""
    @staticmethod
    def process(df: pd.DataFrame) -> pd.DataFrame:
        print(">> データを加工中...")
        df_clean = df.copy()

        if '日付' in df_clean.columns:
            # Zaimの日付形式に合わせて変換
            df_clean['date_obj'] = pd.to_datetime(df_clean['日付'], errors='coerce')
            df_clean['Year'] = df_clean['date_obj'].dt.year
            df_clean['Month'] = df_clean['date_obj'].dt.month
            df_clean['YearMonth'] = df_clean['date_obj'].dt.strftime('%Y-%m')
            df_clean['date_obj'] = df_clean['date_obj'].dt.strftime('%Y-%m-%d')

        for col in ['入金', '出金']:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.replace('¥', '').str.replace(',', '')
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)

        return df_clean.fillna('')

# --- 5. 保存クラス ---
class SheetUploader:
    """Google Sheetsへのアップロード"""
    def __init__(self, config: Config):
        self.config = config
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(self.config.JSON_KEYFILE, self.scope)
        self.client = gspread.authorize(self.creds)

    def upload(self, df: pd.DataFrame):
        print(">> スプレッドシートへアップロード中...")
        try:
            sheet = self.client.open_by_key(self.config.SPREADSHEET_KEY).sheet1
            sheet.clear()
            data = [df.columns.values.tolist()] + df.values.tolist()
            sheet.update(data)
            print(">> 完了")
        except Exception as e:
            print(f"アップロード中にエラーが発生しました: {e}")

# --- メイン実行 ---
def main():
    # Config初期化 (引数なしで.envから読み込み)
    config = Config()
    
    try:
        config.validate() # 設定値のチェック
    except ValueError as e:
        print(e)
        return

    # BrowserManager起動 (headless=False で動作確認推奨)
    with BrowserManager(config, headless=True) as helper:
        
        scraper = ZaimScraper(helper, config)
        try:
            scraper.login()
            raw_df = scraper.fetch_history_data()

            if raw_df is not None:
                processor = DataProcessor()
                clean_df = processor.process(raw_df)

                uploader = SheetUploader(config)
                uploader.upload(clean_df)
            else:
                print("データの取得に失敗したため、処理を中断します。")
        except Exception as e:
            print(f"処理中に予期せぬエラーが発生しました: {e}")

if __name__ == "__main__":
    main()
