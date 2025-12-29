import argparse
import datetime
import logging
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

# main.py からクラスをインポート
# ※ main.py と同じフォルダに置いてください
from main import Config, SheetUploader

# ログ設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class InsightAnalyzer:
    """データを分析して、家計の急変動や使いすぎを検出する"""

    # 定数として定義 (メンテナンス性の向上)
    THRESHOLD_AMOUNT = 5000  # 使いすぎ判定の金額閾値 (円)
    THRESHOLD_RATE = 20      # 使いすぎ判定の比率閾値 (%)

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def analyze_monthly_changes(self, target_month: str) -> pd.DataFrame:
        """
        指定された月(target_month: 'YYYY-MM')とその前月を比較分析する
        """
        logging.info(f"家計インサイトを分析中... (対象: {target_month})")

        # 日付オブジェクトを作成して前月を計算
        try:
            target_date = datetime.datetime.strptime(target_month, "%Y-%m")
            prev_date = target_date - relativedelta(months=1)
            prev_month = prev_date.strftime("%Y-%m")
        except ValueError:
            logging.error("日付フォーマット不正。YYYY-MM 形式で指定してください。")
            return pd.DataFrame()

        logging.info(f"  - 比較対象: {target_month} vs {prev_month}")

        # 1. ピボットテーブル作成
        df_calc = self.df.copy()

        # 対象月と前月のデータのみに絞る（高速化のため）
        df_calc = df_calc[df_calc["YearMonth"].isin([target_month, prev_month])]

        if df_calc.empty:
            logging.warning("  - 指定された期間のデータが存在しません。")
            return pd.DataFrame()

        pivot = df_calc.pivot_table(index="カテゴリ", columns="YearMonth", values="出金", aggfunc="sum", fill_value=0)

        # 必要な列があるか確認
        if target_month not in pivot.columns:
            logging.warning(f"  - 対象月({target_month})のデータがありません。")
            pivot[target_month] = 0
        if prev_month not in pivot.columns:
            # 前月データがない場合は0として比較
            pivot[prev_month] = 0

        # 2. 比較計算
        analysis_df = pd.DataFrame(index=pivot.index)
        analysis_df["当月"] = pivot[target_month]
        analysis_df["前月"] = pivot[prev_month]
        analysis_df["増減額"] = analysis_df["当月"] - analysis_df["前月"]

        # 増減率 (%)
        analysis_df["増減率(%)"] = (
            ((analysis_df["増減額"] / analysis_df["前月"]) * 100).where(analysis_df["前月"] > 0, 0).round(1)
        )

        # 3. 判定ロジック
        # 条件のリスト
        conditions = [
            (analysis_df["増減額"] > self.THRESHOLD_AMOUNT) & (analysis_df["増減率(%)"] > self.THRESHOLD_RATE),
            (analysis_df["増減額"] > 0),
            (analysis_df["増減額"] == 0),
        ]

        # 条件に対応する値のリスト
        choices = ["⚠️使いすぎ", "増加", "-"]

        # どの条件にも当てはまらない場合のデフォルト値 ("減少/維持")
        analysis_df["判定"] = np.select(conditions, choices, default="減少/維持")

        # 4. 整形
        # 増減額の絶対値が大きい順、あるいは増加額が大きい順などでソート
        analysis_df = analysis_df.sort_values(by="増減額", ascending=False).reset_index()

        # カラム名をわかりやすく調整
        analysis_df = analysis_df.rename(columns={"当月": f"{target_month}実績", "前月": f"{prev_month}実績"})

        return analysis_df


def main():
    # --- 引数解析 ---
    parser = argparse.ArgumentParser(description="家計簿データのインサイト分析を行います。")
    parser.add_argument(
        "--month", type=str, help="分析対象の月 (例: 2024-11)。指定しない場合は「先月」が自動選択されます。"
    )
    args = parser.parse_args()

    # --- 設定と準備 ---
    config = Config()
    try:
        config.validate()
    except ValueError as e:
        logging.error(e)
        return

    uploader = SheetUploader(config)

    # 1. データの読み込み (スプレッドシートから)
    df_all = uploader.fetch_all_data()
    if df_all.empty:
        logging.error("データが取得できませんでした。終了します。")
        return

    # 2. 対象月の決定
    if args.month:
        target_month = args.month
    else:
        # 指定がなければ「今日」の「先月」を自動計算
        # 例: 今日が5月なら、4月を対象にする（4月 vs 3月）
        today = datetime.date.today()
        last_month_date = today - relativedelta(months=1)
        target_month = last_month_date.strftime("%Y-%m")
        logging.info(f"対象月が指定されていないため、先月 ({target_month}) を自動選択しました。")

    # 3. 分析実行
    analyzer = InsightAnalyzer(df_all)
    insight_df = analyzer.analyze_monthly_changes(target_month)

    # 4. 結果のアップロード
    if not insight_df.empty:
        # 結果を表示
        print("\n--- 分析結果プレビュー ---")
        print(insight_df.head())
        print("------------------------\n")

        uploader.upload_insight(insight_df)
    else:
        logging.info("分析結果が空のため、アップロードをスキップします。")


if __name__ == "__main__":
    main()
