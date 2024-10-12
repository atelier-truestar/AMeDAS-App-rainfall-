import pandas as pd
import pytest  # type: ignore

from ..streamlit.main import DataProcessor


# テスト用のデータフレームを作成
@pytest.fixture
def input_df():
    data = {
        'ADDRESS': [
            '東京都渋谷区道玄坂2-1-1',
            '大阪府大阪市北区梅田1-2-3',
            '京都府京都市東山区三条1-1',
            '旧字や特殊文字が含まれた住所'
        ]
    }
    return pd.DataFrame(data)

@pytest.fixture
def observatory_df():
    # 必要な観測所データをモックとして用意
    data = {
        'OBSERVATORY_NAME': ['Shibuya', 'Osaka', 'Kyoto'],
        'LATITUDE': [35.658034, 34.705498, 35.011636],
        'LONGITUDE': [139.701636, 135.498302, 135.76803]
    }
    return pd.DataFrame(data)

# テストケース
def test_process_input_data(input_df, observatory_df):
    # DataProcessorクラスのインスタンスを作成
    processor = DataProcessor(input_df, observatory_df)

    # メソッドを実行して処理されたデータを取得
    result_df = processor.process_input_data('ADDRESS')

    # 期待される結果データフレーム
    expected_data = {
        'Pref name': ['東京都', '大阪府', '京都府', 'XXX'],
        'City name': ['渋谷区', '大阪市', '京都市', 'XXX'],
        'Street name': ['道玄坂', '梅田', '三条', 'XXX']
    }
    expected_df = pd.DataFrame(expected_data)

    # アサーション
    pd.testing.assert_frame_equal(result_df[['Pref name', 'City name', 'Street name']], expected_df)