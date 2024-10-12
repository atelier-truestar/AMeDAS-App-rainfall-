import os
import sys

import pandas as pd
import pytest  # type: ignore

pd.set_option('display.max_columns', None)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src/streamlit'))
from main import DataProcessor  # type: ignore  # noqa: E402


# テスト用のデータフレームを読み込み(テストデータ、観測所データ、正解データ)
@pytest.fixture
def read_test_df() -> pd.DataFrame:
    input_test_df = pd.read_csv('input.csv')
    return input_test_df

@pytest.fixture
def read_observatory_df() -> pd.DataFrame:
    observatory_df = pd.read_csv('observatory.csv')
    return observatory_df

@pytest.fixture
def read_correct_df() -> pd.DataFrame:
    correct_df = pd.read_csv('correct.csv')
    return correct_df

# テストケース
def test_process_input_data(read_test_df: pd.DataFrame, read_observatory_df: pd.DataFrame, \
    read_correct_df: pd.DataFrame) -> None:
    # DataProcessorクラスのインスタンスを作成
    processor = DataProcessor(read_test_df, read_observatory_df)

    # メソッドを実行して処理されたデータを取得
    result_df = processor.process_input_data('ADDRESS')

    check_df = result_df[["NEAREST_OBSERVATORY_NAME", "MATCH_LEVEL"]]

    # アサーション
    pd.testing.assert_frame_equal(check_df, read_correct_df)

