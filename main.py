import os
import time
import re
import logging
import traceback
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from selenium_helper.selenium_helper import SeleniumBrowser
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ログ設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

load_dotenv()


# --- 1. 設定管理クラス ---
@dataclass(frozen=True)
class Config:
    ZAIM_EMAIL: str = os.getenv("ZAIM_EMAIL", "")
    ZAIM_PASS: str = os.getenv("ZAIM_PASS", "")
    SPREADSHEET_KEY: str = os.getenv("SPREADSHEET_KEY", "")
    JSON_KEYFILE: str = os.getenv("JSON_KEYFILE", "service_account.json")

    URL_LOGIN: str = "https://auth.zaim.net/"
    URL_HISTORY_BASE: str = "https://zaim.net/money"

    GECKODRIVER_PATH: str = os.getenv("GECKODRIVER_PATH", "./geckodriver")
    FIREFOX_BINARY_PATH: str = os.getenv("FIREFOX_BINARY_PATH", "")
    FIREFOX_PROFILE_PATH: str = os.getenv("FIREFOX_PROFILE_PATH", "")

    def validate(self):
        if not all([self.ZAIM_EMAIL, self.ZAIM_PASS, self.SPREADSHEET_KEY]):
            raise ValueError("必要な環境変数が設定されていません。")


# --- 2. ブラウザ管理クラス ---
class BrowserManager:
    def __init__(self, config: Config, headless: bool = True):
        self.config = config
        self.headless = headless
        self.helper_browser: Optional[SeleniumBrowser] = None

    def __enter__(self) -> SeleniumBrowser:
        logging.info("Firefoxを起動中...")
        browser_setting = {
            "browser_path": self.config.FIREFOX_BINARY_PATH,
            "browser_profile": self.config.FIREFOX_PROFILE_PATH,
        }
        self.helper_browser = SeleniumBrowser(
            geckodriver_path=self.config.GECKODRIVER_PATH,
            headless=self.headless,
            browser_setting=browser_setting,
            set_size=False,
        )

        # 数百件の明細を一気に入れるため、超縦長サイズに設定
        target_height = 15000
        logging.info(f"ウィンドウサイズを拡張します (1280x{target_height})")
        self.helper_browser.browser.set_window_size(1280, target_height)

        return self.helper_browser

    def __exit__(self, exc_type, exc_value, traceback):
        if self.helper_browser:
            logging.info("Firefoxを終了します")
            self.helper_browser.close_selenium()


# --- 3. ZaimScraper クラス---
class ZaimScraper:
    def __init__(self, helper: SeleniumBrowser, config: Config):
        self.helper = helper
        self.driver = helper.browser
        self.config = config
        self.wait = WebDriverWait(self.driver, 20)

    def login(self):
        logging.info("Zaimへログインを開始します...")
        self.helper.recur_selenium_get(self.config.URL_LOGIN)
        time.sleep(3)

        current_url = self.driver.current_url
        if "auth.zaim.net" in current_url:
            try:
                self.wait.until(EC.visibility_of_element_located((By.ID, "UserEmail"))).send_keys(
                    self.config.ZAIM_EMAIL
                )
                self.wait.until(EC.visibility_of_element_located((By.ID, "UserPassword"))).send_keys(
                    self.config.ZAIM_PASS
                )
                self.driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
                time.sleep(5)
            except Exception as e:
                logging.warning(f"Zaimログインフォームの操作中にエラー（すでにログイン済み等の可能性）: {e}")

        if "id.kufu.jp" in self.driver.current_url:
            logging.info("くふうアカウント画面を検知、再ログインします。")
            self._login_kufu_account()

        time.sleep(5)
        if "money" in self.driver.current_url or "home" in self.driver.current_url:
            logging.info("ログイン成功")

    def _login_kufu_account(self):
        try:
            try:
                email = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input[type='email']")))
                email.clear()
                email.send_keys(self.config.ZAIM_EMAIL)
            except Exception as e:
                logging.debug(f"くふうアカウント メール入力スキップ (自動入力済みの可能性): {e}")

            pwd = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input[type='password']")))
            pwd.clear()
            pwd.send_keys(self.config.ZAIM_PASS)

            self.driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']").click()
            time.sleep(5)
        except Exception as e:
            logging.error(f"くふうアカウントログインエラー: {e}")
            raise

    def fetch_data_loop(self, months: int = 3) -> Optional[pd.DataFrame]:
        all_dfs = []
        # datetime.date.today() ではなく pd.Timestamp.today() を使用
        # pd.DateOffset は pandas の Timestamp 型との計算に最適化されているため
        today = pd.Timestamp.today()

        for i in range(months):
            target_date = today - pd.DateOffset(months=i)
            month_param = target_date.strftime("%Y%m")

            url = f"{self.config.URL_HISTORY_BASE}?month={month_param}"
            logging.info(f"[{i+1}/{months}] {target_date.strftime('%Y年%m月')} のデータを取得中... ({url})")

            df = self._scrape_one_shot(url)

            if df is not None and not df.empty:
                df["ScrapedYear"] = target_date.year
                all_dfs.append(df)
            else:
                logging.info("データなし")

            time.sleep(3)

        if not all_dfs:
            return None

        return pd.concat(all_dfs, ignore_index=True)

    def _scrape_one_shot(self, url: str) -> Optional[pd.DataFrame]:
        """[縦長ウィンドウ版] スクロールせずに一括解析する（念のため末尾へ一度移動）"""
        try:
            self.helper.recur_selenium_get(url)

            try:
                container = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[class^='SearchResult-module__list']"))
                )
                time.sleep(5)  # 描画待ち時間を長めに確保
            except Exception as e:
                logging.error(f"リスト要素が見つかりませんでした: {e}")
                return None

            # 念のため、JavaScriptで一番下へ一度だけ飛ばして、遅延ロードの残りを拾う
            try:
                self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", container)
                time.sleep(2)
            except Exception as e:
                logging.warning(f"強制スクロール実行時にエラー（無視して続行）: {e}")

            # 解析（画面に見えているもの全てを取得）
            scraped_data = {}
            self._parse_current_view(scraped_data)

            logging.info(f"合計 {len(scraped_data)} 件のデータを抽出完了")
            return pd.DataFrame(list(scraped_data.values()))

        except Exception as e:
            logging.error(f"解析エラー: {e}")
            return None

    def _parse_current_view(self, data_store: dict):
        """現在のDOMにある行を解析"""
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            rows = soup.find_all("div", class_=re.compile(r"SearchResult-module__body"))

            for row in rows:
                try:
                    zaim_id = ""
                    link_elm = row.find(attrs={"data-url": True})
                    if link_elm:
                        match = re.search(r"/money/(\d+)", link_elm["data-url"])
                        if match:
                            zaim_id = match.group(1)

                    if not zaim_id:
                        continue
                    if zaim_id in data_store:
                        continue

                    date_div = row.find("div", class_=re.compile(r"SearchResult-module__date"))
                    date_text = date_div.get_text(strip=True) if date_div else ""

                    cat_div = row.find("div", class_=re.compile(r"SearchResult-module__category"))
                    cat_text = cat_div.get_text(strip=True) if cat_div else ""

                    price_div = row.find("div", class_=re.compile(r"SearchResult-module__price"))
                    price_text = price_div.get_text(strip=True) if price_div else "0"

                    from_div = row.find("div", class_=re.compile(r"SearchResult-module__fromAccount"))
                    from_img = from_div.find("img") if from_div else None
                    from_text = from_img.get("alt") if from_img else ""

                    to_div = row.find("div", class_=re.compile(r"SearchResult-module__toAccount"))
                    to_img = to_div.find("img") if to_div else None
                    to_text = to_img.get("alt") if to_img else ""

                    place_div = row.find("div", class_=re.compile(r"SearchResult-module__place"))
                    place_text = place_div.get_text(strip=True) if place_div else ""

                    name_div = row.find("div", class_=re.compile(r"SearchResult-module__name"))
                    name_text = name_div.get_text(strip=True) if name_div else ""

                    data_store[zaim_id] = {
                        "zaim_id": zaim_id,
                        "日付": date_text,
                        "カテゴリ": cat_text,
                        "金額": price_text,
                        "出金元": from_text,
                        "入金先": to_text,
                        "お店": place_text,
                        "品名": name_text,
                    }
                except Exception as e:
                    logging.error(f"行データの解析エラー: {e}")
                    continue
        except Exception as e:
            logging.error(f"ビュー全体の解析エラー: {e}")


# --- 4. データ加工クラス ---
class DataProcessor:
    @staticmethod
    def process(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("データを加工・結合中...")
        df_clean = df.copy()

        # 日付処理
        if "日付" in df_clean.columns:

            def parse_date(row):
                try:
                    text = str(row["日付"])
                    # 「（祝）」「（振替休日）」など任意長の括弧内文字列にも対応
                    text = re.sub(r"（[^）]+）", "", text).strip()
                    text = text.replace("(", "").replace(")", "")

                    return pd.to_datetime(f"{row['ScrapedYear']}年{text}", format="%Y年%m月%d日", errors="coerce")
                except:
                    return pd.NaT

            df_clean["date_obj"] = df_clean.apply(parse_date, axis=1)
            df_clean["Year"] = df_clean["date_obj"].dt.year
            df_clean["Month"] = df_clean["date_obj"].dt.month
            df_clean["YearMonth"] = df_clean["date_obj"].dt.strftime("%Y-%m")
            df_clean["date_obj"] = df_clean["date_obj"].dt.strftime("%Y-%m-%d")

        # カテゴリのクリーニング
        if "カテゴリ" in df_clean.columns:

            def clean_cat(text):
                jp_match = re.search(r"[^\x00-\x7F]+", text)
                if jp_match:
                    return text[jp_match.start() :]
                return text

            df_clean["カテゴリ"] = df_clean["カテゴリ"].apply(clean_cat)

        # 金額処理
        if "金額" in df_clean.columns:
            df_clean["金額"] = df_clean["金額"].astype(str).str.replace("¥", "").str.replace(",", "")
            df_clean["金額"] = pd.to_numeric(df_clean["金額"], errors="coerce").fillna(0)

        # 出金と入金ロジックの整理
        # 入金先がある場合は「入金」列、出金元がある場合は「出金」列に金額を入れる
        df_clean["入金"] = 0
        df_clean["出金"] = 0

        # '入金先' カラムが存在し、かつ値が入っている(NaNでも空文字でもない)場合は「入金」扱い
        # それ以外は「出金」扱いとする
        if "入金先" in df_clean.columns:
            mask_income = df_clean["入金先"].notna() & (df_clean["入金先"] != "")

            # 入金フラグがTrueの行は、金額を「入金」列へコピー
            df_clean.loc[mask_income, "入金"] = df_clean.loc[mask_income, "金額"]

            # 入金フラグがFalseの行は、金額を「出金」列へコピー
            df_clean.loc[~mask_income, "出金"] = df_clean.loc[~mask_income, "金額"]
        else:
            # 入金先カラムがない場合は全て出金扱い（安全策）
            df_clean["出金"] = df_clean["金額"]

        return df_clean.fillna("")


# --- 5. SheetUploader クラス ---
class SheetUploader:
    def __init__(self, config: Config):
        self.config = config
        self.scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(self.config.JSON_KEYFILE, self.scope)
        self.client = gspread.authorize(self.creds)

    def upload(self, df_new: pd.DataFrame):
        logging.info("スプレッドシートへ安全にアップロード中（重複チェック）...")
        try:
            sheet = self.client.open_by_key(self.config.SPREADSHEET_KEY).sheet1

            # 1. 既存データを全取得
            existing_records = sheet.get_all_values()

            if existing_records:
                # ヘッダーとデータに分離
                headers = existing_records[0]
                df_old = pd.DataFrame(existing_records[1:], columns=headers)

                # 型合わせ
                df_new["zaim_id"] = df_new["zaim_id"].astype(str)
                if "zaim_id" in df_old.columns:
                    df_old["zaim_id"] = df_old["zaim_id"].astype(str)

                logging.info(f"既存データ: {len(df_old)}件, 新規データ: {len(df_new)}件")

                # 2. 結合
                df_combined = pd.concat([df_old, df_new], ignore_index=True)

                # 3. 重複排除
                if "zaim_id" in df_combined.columns:
                    df_combined = df_combined[df_combined["zaim_id"] != ""]
                    df_final = df_combined.drop_duplicates(subset=["zaim_id"], keep="last").copy()
                else:
                    df_final = df_combined.copy()

                logging.info(f"重複排除後の件数: {len(df_final)}件")
            else:
                df_final = df_new.copy()

            # 4. 日付順にソート
            if "date_obj" in df_final.columns:
                df_final["date_obj"] = pd.to_datetime(df_final["date_obj"], errors="coerce")
                df_final = df_final.sort_values(by="date_obj", ascending=False)
                df_final["date_obj"] = df_final["date_obj"].dt.strftime("%Y-%m-%d")

            # 5. 書き込み
            sheet.clear()
            sheet.update([df_final.columns.values.tolist()] + df_final.values.tolist())
            logging.info("アップロード完了")

        except Exception as e:
            logging.error(f"アップロードエラー: {e}")
            logging.error(traceback.format_exc())


# --- メイン実行 ---
def main():
    config = Config()
    try:
        config.validate()
    except ValueError as e:
        logging.error(e)
        return

    with BrowserManager(config, headless=True) as helper:
        scraper = ZaimScraper(helper, config)

        try:
            scraper.login()

            # 3ヶ月分取得
            raw_df = scraper.fetch_data_loop(months=3)

            if raw_df is not None and not raw_df.empty:
                processor = DataProcessor()
                clean_df = processor.process(raw_df)

                uploader = SheetUploader(config)
                uploader.upload(clean_df)
            else:
                logging.info("データの取得に失敗しました。")

        except Exception as e:
            logging.error(f"処理中にエラーが発生: {e}")


if __name__ == "__main__":
    main()
