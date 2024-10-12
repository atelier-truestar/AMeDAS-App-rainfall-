"""モジュール"""
import os
import re
import time
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import snowflake.permissions as permissions  # type: ignore
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException

import streamlit as st


class DataFetcher:
   """DataFetcherクラス

   データベースからデータを取得するためのクラス
   """

   def __init__(self, session: Session) -> None:
      """コンストラクタ

      :arg session: データベースセッション
      """
      try:
         self.session: Session = session
      except Exception as e:
         st.error("セッションの初期化に失敗しました。")
         st.error(f"エラー内容: {str(e)}")

   def get_daily_nearest_observatory(self) -> pd.DataFrame:
      """日次の最寄り観測所データを取得する

      :return: 最寄りの観測所データのDataFrame
      """
      query: str = "SELECT * FROM PUBLIC.DAILY_OBSERVATORY_RAINFALL"

      try:
         daily_amedas_df = self.session.sql(query).to_pandas()
         return daily_amedas_df

      except Exception as e:
         st.error("最寄り観測所データの取得に失敗しました。")
         st.error(f"エラー内容: {str(e)}")
         return pd.DataFrame()  # 失敗時は空のDataFrameを返す

   def get_daily_amedas(self) -> pd.DataFrame:
      """日次のAMeDASデータを取得する

      :return: AMeDASデータのDataFrame
      """
      try:
         # クエリの構築
         query = "SELECT OBSERVATORY_NAME, DATE, RAINFALL_DAILY_TOTAL FROM PUBLIC.DAILY_AMEDAS"
         # キャッシュキーの作成
         cache_key = 'amedasdata'
         if cache_key not in st.session_state:
               st.session_state[cache_key] = self.session.sql(query).to_pandas()
         return st.session_state[cache_key]

      except Exception as e:
         st.error("AMeDASデータの取得に失敗しました。")
         st.error(f"エラー内容: {str(e)}")
         return pd.DataFrame()  # 失敗時は空のDataFrameを返す

   @staticmethod
   def get_min_max_date() -> Tuple[str, str]:
      """日次のデータの最小日付と最大日付を取得する

      :arg session: データベースセッション
      :return: 最小日付と最大日付のタプル
      """
      query: str = "SELECT MIN(DATE), MAX(DATE) FROM PUBLIC.DAILY_AMEDAS"
      try:
         min_date, max_date = session.sql(query).to_pandas().iloc[0]
         return min_date, max_date
      except Exception as e:
         st.error("日付の取得に失敗しました。")
         st.error(f"エラー内容: {str(e)}")
         return "", ""  # 失敗時は空文字列を返す

class DataProcessor:
   """DataProcessorクラス

   データの処理と加工を行うクラス
   """
   replacement_patterns: Dict[str, str] = {
      r"字|大字|小字": "",
      r"鬮野川|くじ野川|くじの川": "くじ野川",
      r"通り|とおり": "通り",
      r"柿碕町|柿さき町": "柿碕町",
      r"埠頭|ふ頭": "埠頭",
      r"番町|番丁": "番町",
      r"大冝|大宜": "大宜",
      r"穝|さい": "穝",
      r"杁|えぶり": "杁",
      r"薭|稗|ひえ|ヒエ": "稗",
      r"上ル|上る": "上る",
      r"下ル|下る": "下る",
      r"四ツ谷|四谷": "四谷",
      r"[之ノの]": "",
      r"[ｹヶケが]": "が",
      r"[ｶヵカか力]": "か",
      r"[ﾂッツっつ]": "つ",
      r"[ニ二]": "二",
      r"[ハ八]": "八"
   }

   def __init__(self, input_df: pd.DataFrame, observatory_df: pd.DataFrame):
      """コンストラクタ

      :arg input_df: 入力データのDataFrame
      :arg observatory_df: 観測所データのDataFrame
      """
      self.input_df: pd.DataFrame = input_df
      self.observatory_df: pd.DataFrame = observatory_df

   def process_input_data(self, address_column: str) -> pd.DataFrame:
      """入力データを処理し、住所データを正規化する

      :arg address_column: 住所データが含まれる列名
      :return: 処理された住所データのDataFrame
      """
      try:
         # 特定の列が存在しない場合のエラーをキャッチ
         self.input_df['CLEANSED_ADDRESS'] = self.input_df[address_column]\
            .str.replace(r'[!?/:@[\]`{\}~ 　]','',regex=True)

      except KeyError as e:
         st.error(f"指定された列名 '{address_column}' が存在しません。")
         st.error(f"エラー内容: {str(e)}")
         return

      try:
         # 住所データの処理
         self.input_df['CLEANSED_ADDRESS'] = self.input_df['CLEANSED_ADDRESS'].apply(self.replace_old_kanji)
         self.input_df['CLEANSED_ADDRESS'] = self.input_df['CLEANSED_ADDRESS'].apply\
            (lambda x: self.replace_patterns(x, self.replacement_patterns))
      except AttributeError as e:
         st.error("住所データの処理中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")
         return

      pattern: str = (
         r'^(?P<Pref_name>(?:東京都|京都府|大阪府|.+?[都道府県]))?'  # 都道府県名が省略されていてもOK
         r'(?P<City_name>(?:(?:京都|札幌|福岡|田村|東村山|武蔵村山|羽村|十日町|野々市|大町|蒲郡|四日市|大和郡山|廿日市|大村)市|.\
            +?郡(?:玉村|大町|.+?)[町村]|.+?市.+?区|.+?[市区町村]))?'  # 市区郡名が省略されていてもOK
         r'(?P<Street_name>.*)'  # 番地や町名
      )

      try:
         # 正規表現での住所分割
         splitted_df: pd.DataFrame = self.input_df['CLEANSED_ADDRESS'].str.extract(pattern)
      except Exception as e:
         st.error("住所の正規表現解析中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")
         return

      # 市区町村名と都道府県名が同一の場合の処理
      try:
         splitted_df.loc[(splitted_df['City_name'] == splitted_df['Pref_name']), 'City_name'] = 'XXX'
         splitted_df.columns = ['Pref name', 'City name', 'Street name']
         splitted_df['Street name'] = splitted_df['Street name'].apply(self.normalize_address)
         splitted_df['Street name'] = splitted_df['Street name'].astype(str).str.replace(r'\d+.*', '', regex=True)
         splitted_df = splitted_df.fillna('XXX')
         splitted_df.replace('', 'XXX', inplace=True)
      except Exception as e:
         st.error("住所データの加工中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")
         return

      return self.join_dfs(splitted_df)

   @staticmethod
   def replace_old_kanji(address: str) -> str:
      """旧字体を新字体に置換する

      :arg address: 置換前の住所文字列
      :return: 置換後の住所文字列
      """
      # 旧字体と新字体のリスト
      jis_old_kanji: List[str] = (
         "亞,圍,壹,榮,驛,應,櫻,假,會,懷,覺,樂,陷,歡,氣,戲,據,挾,區,徑,溪,輕,藝,儉,圈,權,嚴,恆,國,齋,雜,蠶,殘,兒,實,釋,從,縱,敍,燒,條,剩,壤,釀,眞,盡,醉,髓,聲,竊,"
         "淺,錢,禪,爭,插,騷,屬,對,滯,擇,單,斷,癡,鑄,敕,鐵,傳,黨,鬪,屆,腦,廢,發,蠻,拂,邊,瓣,寶,沒,滿,藥,餘,樣,亂,兩,禮,靈,爐,灣,惡,醫,飮,營,圓,歐,奧,價,繪,擴,學,"
         "罐,勸,觀,歸,犧,擧,狹,驅,莖,經,繼,缺,劍,檢,顯,廣,鑛,碎,劑,參,慘,絲,辭,舍,壽,澁,肅,將,證,乘,疊,孃,觸,寢,圖,穗,樞,齊,攝,戰,潛,雙,莊,裝,藏,續,體,臺,澤,膽,"
         "彈,蟲,廳,鎭,點,燈,盜,獨,貳,霸,賣,髮,祕,佛,變,辯,豐,飜,默,與,譽,謠,覽,獵,勵,齡,勞,壓,爲,隱,衞,鹽,毆,穩,畫,壞,殼,嶽,卷,關,顏,僞,舊,峽,曉,勳,惠,螢,鷄,縣,"
         "險,獻,驗,效,號,濟,册,棧,贊,齒,濕,寫,收,獸,處,稱,奬,淨,繩,讓,囑,愼,粹,隨,數,靜,專,踐,纖,壯,搜,總,臟,墮,帶,瀧,擔,團,遲,晝,聽,遞,轉,當,稻,讀,惱,拜,麥,拔,"
         "濱,竝,辨,舖,襃,萬,譯,豫,搖,來,龍,壘,隸,戀,樓,鰺,鶯,蠣,攪,竈,灌,諫,頸,礦,蘂,靱,賤,壺,礪,檮,濤,邇,蠅,檜,儘,藪,籠,彌,麩,栁,塚,淵,舟,會"
      ).split(",")

      jis_new_kanji: List[str] = (
         "亜,囲,壱,栄,駅,応,桜,仮,会,懐,覚,楽,陥,歓,気,戯,拠,挟,区,径,渓,軽,芸,倹,圏,権,厳,恒,国,斎,雑,蚕,残,児,実,釈,従,縦,叙,焼,条,剰,壌,醸,真,尽,酔,髄,声,窃,"
         "浅,銭,禅,争,挿,騒,属,対,滞,択,単,断,痴,鋳,勅,鉄,伝,党,闘,届,脳,廃,発,蛮,払,辺,弁,宝,没,満,薬,余,様,乱,両,礼,霊,炉,湾,悪,医,飲,営,円,欧,奥,価,絵,拡,学,"
         "缶,勧,観,帰,犠,挙,狭,駆,茎,経,継,欠,剣,検,顕,広,鉱,砕,剤,参,惨,糸,辞,舎,寿,渋,粛,将,証,乗,畳,嬢,触,寝,図,穂,枢,斉,摂,戦,潜,双,荘,装,蔵,続,体,台,沢,胆,"
         "弾,虫,庁,鎮,点,灯,盗,独,弐,覇,売,髪,秘,仏,変,弁,豊,翻,黙,与,誉,謡,覧,猟,励,齢,労,圧,為,隠,衛,塩,殴,穏,画,壊,殻,岳,巻,関,顔,偽,旧,峡,暁,勲,恵,蛍,鶏,県,"
         "険,献,験,効,号,済,冊,桟,賛,歯,湿,写,収,獣,処,称,奨,浄,縄,譲,嘱,慎,粋,随,数,静,専,践,繊,壮,捜,総,臓,堕,帯,滝,担,団,遅,昼,聴,逓,転,当,稲,読,悩,拝,麦,抜,"
         "浜,並,弁,舗,褒,万,訳,予,揺,来,竜,塁,隷,恋,楼,鯵,鴬,蛎,撹,竃,潅,諌,頚,砿,蕊,靭,賎,壷,砺,梼,涛,迩,蝿,桧,侭,薮,篭,弥,麸,柳,塚,渕,船,曽"
      ).split(",")
      # 旧字体から新字体へのマッピング辞書を作成
      kanji_mapping: Dict[str, str] = dict(zip(jis_old_kanji, jis_new_kanji))

      try:
         for old, new in kanji_mapping.items():
               address = address.replace(old, new)
      except Exception as e:
         st.error("旧字体の変換中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")
      return address

   @staticmethod
   def replace_patterns(address: str, patterns: Dict[str, str]) -> str:
      """住所文字列に対して一連の置換パターンを適用する

      :arg address: 置換前の住所文字列
      :arg patterns: 適用する置換パターンの辞書
      :return: 置換後の住所文字列
      """
      try:
         for pattern, replacement in patterns.items():
               address = re.sub(pattern, replacement, address)
         return address
      except Exception as e:
         # 置換パターンの適用中にエラーが発生した場合
         st.error("置換パターンの適用中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")
         return address

   @staticmethod
   def normalize_address(address: str) -> str:
      """住所文字列を正規化する（漢数字をアラビア数字に変換など）

      :arg address: 正規化前の住所文字列
      :return: 正規化後の住所文字列
      """
      try:
         # 正規表現による住所の分割
         match = re.match(r'(.+?)((?:[一二三四五六七八九十〇0-9]+[^0-9]*)+)$', address)
         if not match:
               return address

         town_name, number_part = match.groups()
         numbers: List[str] = re.findall(r'([一二三四五六七八九十〇]+|[0-9]+)', number_part)

         # 漢数字をアラビア数字に変換する
         normalized_numbers = [DataProcessor.convert_kanji_to_number(num) for num in numbers]

         normalized: str = f"{town_name}{'-'.join(normalized_numbers)}"
         return normalized
      except Exception as e:
         # 正規化中にエラーが発生した場合
         st.error("住所の正規化中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")
         return address

   @staticmethod
   def convert_kanji_to_number(num: str) -> str:
      """漢数字をアラビア数字に変換する"""
      kanji_to_number: Dict[str, str] = {
      '一': '1', '二': '2', '三': '3', '四': '4', '五': '5',
      '六': '6', '七': '7', '八': '8', '九': '9', '十': '10',
      '〇': '0'
      }
      try:
         if num in kanji_to_number:
               return kanji_to_number[num]
         elif all(char in kanji_to_number for char in num):
               normalized_num = ''
               for char in num:
                  if char == '十':
                     if normalized_num == '':
                           normalized_num += '10'
                     elif normalized_num[-1] == '1':
                           normalized_num = normalized_num[:-1] + '10'
                     else:
                           normalized_num = normalized_num[:-1] + str(int(normalized_num[-1]) * 10)
                  else:
                     normalized_num += kanji_to_number[char]
               return normalized_num
         else:
               return num
      except Exception as e:
         st.error("漢数字の変換中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")
         raise e

   def find_matching_row(self, row: pd.Series, observatory_df: pd.DataFrame) -> pd.Series:
      """入力された住所に最も近い観測所を見つける

      :arg row: 処理する住所データの行
      :arg observatory_df: 観測所データのDataFrame
      :return: マッチした観測所データと一致レベル
      """
      try:
         street_match = self._find_street_match(row, observatory_df)
         city_match = self._find_city_match(row, observatory_df)
         prefecture_match = self._find_prefecture_match(row, observatory_df)

         if not prefecture_match.empty and not city_match.empty and not street_match.empty: # Pref ⭕️ City ⭕️ Street ⭕️
               return self._handle_all_matches(row, street_match, city_match, prefecture_match)
         elif prefecture_match.empty and not city_match.empty and not street_match.empty:  # Pref ❌ City ⭕️ Street ⭕️
               return self._handle_city_street_match(row, street_match, city_match)
         elif not prefecture_match.empty and city_match.empty and not street_match.empty: # Pref ⭕️ City ❌ Street ⭕️
               return self._handle_prefecture_street_match(row, street_match, prefecture_match)
         elif not prefecture_match.empty and not city_match.empty and street_match.empty: # Pref ⭕️ City ⭕️ Street ❌
               return self._handle_prefecture_city_match(row, city_match, prefecture_match)
         elif not prefecture_match.empty and city_match.empty and street_match.empty: # Pref ⭕️ City ❌ Street ❌
               return self._create_matched_row(prefecture_match.iloc[0], '都道府県レベル')
         elif prefecture_match.empty and not city_match.empty and street_match.empty: # Pref ❌ City ⭕️ Street ❌
               return self._handle_only_city_match(city_match)
         elif prefecture_match.empty and city_match.empty and not street_match.empty: # Pref ❌ City ❌ Street ⭕️
               return self._handle_only_street_match(street_match)
         else: # Pref ❌ City ❌ Street ❌
               return self._create_error_row('住所形式が不適切です')

      except Exception:
         return self._create_error_row('住所形式が不適切です')

   def _find_street_match(self, row: pd.Series, observatory_df: pd.DataFrame) -> pd.DataFrame:
      # 町丁目名が含まれる行を返す
      return observatory_df[observatory_df['ADDRESS_NAME'].str.contains(row['Street name'], na=False)]

   def _find_city_match(self, row: pd.Series, observatory_df: pd.DataFrame) -> pd.DataFrame:
      # 市区町村名が含まれる行を返す
      city_name_parts = self._split_city_name(row['City name'])
      return observatory_df[
         observatory_df['ADDRESS_NAME'].str.contains(city_name_parts[0], na=False) |
         observatory_df['ADDRESS_NAME'].str.contains(city_name_parts[1], na=False)
      ]

   def _find_prefecture_match(self, row: pd.Series, observatory_df: pd.DataFrame) -> pd.DataFrame:
      # 都道府県名が含まれる行を返す
      return observatory_df[observatory_df['ADDRESS_NAME'].str.contains(row['Pref name'], na=False)]

   def _split_city_name(self, city_name: str) -> Tuple[str, str]:
      # 市区町村名を分割する
      if '郡' in city_name:
         parts = city_name.split('郡')
         return parts[0], re.split(r'[市町村]', parts[1])[0]
      elif '区' in city_name:
         parts = city_name.split('区')
         return parts[0], parts[0]
      else:
         return city_name, city_name

   def _handle_all_matches(self, row: pd.Series, street_match: pd.DataFrame, \
      city_match: pd.DataFrame, prefecture_match: pd.DataFrame) -> pd.Series:
      # Pref ⭕️ City ⭕️ Street ⭕️
      pcs_match = street_match[
         street_match['ADDRESS_NAME'].str.contains(row['City name'], na=False) &
         street_match['ADDRESS_NAME'].str.contains(row['Pref name'], na=False)
      ]
      pc_match = city_match[city_match['ADDRESS_NAME'].str.contains(row['Pref name'], na=False)]
      ps_match = street_match[street_match['ADDRESS_NAME'].str.contains(row['Pref name'], na=False)]
      cs_match = street_match['ADDRESS_NAME'].str.contains(row['City name'], na=False)

      if not pcs_match.empty:
         return self._create_matched_row(pcs_match.iloc[0], '町丁目レベル')
      elif not pc_match.empty:
         return self._create_matched_row(pc_match.iloc[0], '市区郡レベル')
      elif not ps_match.empty:
         return self._handle_prefecture_street_match(row, ps_match, prefecture_match)
      elif cs_match.any():
         return self._handle_city_street_match(row, street_match, city_match)
      else:
         return self._handle_fallback_match(street_match, city_match, prefecture_match)

   def _handle_city_street_match(self, row: pd.Series, street_match: pd.DataFrame, \
      city_match: pd.DataFrame) -> pd.Series:
      # Pref ❌ City ⭕️ Street ⭕️
      cs_match = street_match[street_match['ADDRESS_NAME'].str.contains(row['City name'], na=False)]
      if not cs_match.empty:
         return self._create_matched_row(cs_match.iloc[0], '町丁目レベル')
      else:
         return self._handle_fallback_match(street_match, city_match, pd.DataFrame())

   def _handle_prefecture_street_match(self, row: pd.Series, \
      street_match: pd.DataFrame, prefecture_match: pd.DataFrame) -> pd.Series:
      # Pref ⭕️ City ❌ Street ⭕️
      ps_match = street_match[street_match['ADDRESS_NAME'].str.contains(row['Pref name'], na=False)]
      if not ps_match.empty:
         if len(ps_match) == 1:
               return self._create_matched_row(ps_match.iloc[0], '町丁目レベル')
         else:
               return self._create_matched_row(ps_match.iloc[0], \
                  '県と町丁目がヒットしましたが、2件以上あるので特定できていません')
      else:
         return self._handle_fallback_match(street_match, pd.DataFrame(), prefecture_match)

   def _handle_prefecture_city_match(self, row: pd.Series, \
      city_match: pd.DataFrame, prefecture_match: pd.DataFrame) -> pd.Series:
      # Pref ⭕️ City ⭕️ Street ❌
      pc_match = city_match[city_match['ADDRESS_NAME'].str.contains(row['Pref name'], na=False)]
      if not pc_match.empty:
         return self._create_matched_row(pc_match.iloc[0], '市区郡レベル')
      else:
         return self._handle_fallback_match(pd.DataFrame(), city_match, prefecture_match)

   def _handle_only_city_match(self, city_match: pd.DataFrame) -> pd.Series:
      # Pref ❌ City ⭕️ Street ❌
      if len(city_match) == 1:
         return self._create_matched_row(city_match.iloc[0], '市区郡レベル')
      else:
         return self._create_matched_row(city_match.iloc[0],
                                         '市区郡のみがヒットしましたが、2件以上あるので特定できていません')

   def _handle_only_street_match(self, street_match: pd.DataFrame) -> pd.Series:
      # Pref ❌ City ❌ Street ⭕️
      if len(street_match) == 1:
         return self._create_matched_row(street_match.iloc[0], '町丁目レベル')
      else:
         return self._create_matched_row(street_match.iloc[0], \
            '町丁目のみがヒットしましたが、2件以上あるので特定できていません')

   def _handle_fallback_match(self, street_match: pd.DataFrame, \
      city_match: pd.DataFrame, prefecture_match: pd.DataFrame) -> pd.Series:
      # Pref ❌ City ❌ Street ❌
      if len(street_match) == 1:
         return self._create_matched_row(street_match.iloc[0], '町丁目レベル')
      elif len(city_match) == 1:
         return self._create_matched_row(city_match.iloc[0], '市区郡レベル')
      elif not prefecture_match.empty:
         return self._create_matched_row(prefecture_match.iloc[0], '都道府県レベル')
      else:
         return self._create_error_row('住所形式が不適切です')

   def _create_matched_row(self, matched_row: pd.Series, match_level: str) -> pd.Series:
      # マッチした行にMATCH_LEVEL列を追加
      result = matched_row.copy()
      result['MATCH_LEVEL'] = match_level
      return result

   def _create_error_row(self, error_message: str) -> pd.Series:
      # エラーメッセージを含む行を作成
      matched_row = pd.Series([np.nan] * (len(self.observatory_df.columns) + 1),
                              index=self.observatory_df.columns.tolist() + ['MATCH_LEVEL'])
      matched_row['NEAREST_OBSERVATORY'] = 'XXX'
      matched_row['MATCH_LEVEL'] = error_message
      return matched_row

   def join_dfs(self, splitted_df: pd.DataFrame) -> pd.DataFrame:
      """分割された住所データと観測所データを結合する

      :arg splitted_df: 分割された住所データのDataFrame
      :return: 結合されたDataFrame
      """
      try:
         # splitted_dfの各行に対してfind_matching_rowを適用
         matching_rows: pd.DataFrame = splitted_df.apply\
            (lambda row: self.find_matching_row(row, self.observatory_df), axis=1)

         try:
            # 結合処理
            result_df = pd.concat([splitted_df.reset_index(drop=True),
                                 matching_rows.reset_index(drop=True)],
                                 axis=1)
            return result_df

         except Exception as e:
            # 結合処理中にエラーが発生した場合
            st.error("データフレームの結合中にエラーが発生しました。")
            st.error(f"エラー内容: {str(e)}")
            return splitted_df  # 結合が失敗した場合は、元の分割データを返す

      except Exception as e:
         # apply関数でエラーが発生した場合
         st.error("観測所データとの照合中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")
         return splitted_df  # エラーが発生した場合、元の分割データを返す

class UIHandler:
   """UIHandlerクラス

   Streamlitを使用したUIの処理を行うクラス
   """
   def __init__(self, session: Session) -> None:
      """コンストラクタ

      :arg session: データベースセッション
      """
      try:
         self.session: Session = session
      except Exception as e:
         st.error("セッションの初期化に失敗しました。")
         st.error(f"エラー内容: {str(e)}")

   def setup_page(self) -> None:
      """ページの基本設定を行う"""
      try:
         st.set_page_config(
               page_title="AMeDAS Data Maker",  # ページのタイトルを設定
               page_icon="🌤",  # ページのアイコンを設定
               layout="wide"  # ページのレイアウトを広めに設定
         )
      except Exception as e:
         st.error("ページ設定中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")

      # CSSファイルの読み込み
      try:
         css_file = os.path.join(os.path.dirname(__file__), 'style.css')
         with open(css_file, 'r', encoding='utf-8') as f:
               css = f.read()
         st.markdown(f'<style>{css}</style>', unsafe_allow_html=True)
      except Exception as e:
         st.error("CSSファイルの読み込み中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")

      st.markdown("# ☔ AMeDAS Data Maker (Specialized in RAINFALL) ☔")
      st.markdown("##### 選択したテーブルの住所データから降水量データを紐づけするアプリ")

   def select_table(self, amedas_maker: 'AMeDASDataMaker') -> bool:
      """読み込むテーブルを選択するUIを表示する"""
      st.subheader("1️⃣ 読み込むテーブルの選択")
      try:
         is_input_table_set = amedas_maker.check_input_table_ref_existence()
      except Exception as e:
         st.error("テーブル参照の確認中にエラーが発生しました。")
         st.error(f"エラー内容: {str(e)}")
         return False

      if st.button("テーブルの選択"):
         try:
            for key in list(st.session_state.keys()):
               del st.session_state[key]
            permissions.request_reference(amedas_maker.input_table_ref)

         except Exception as e:
            st.error("テーブル選択中にエラーが発生しました。")
            st.error(f"エラー内容: {str(e)}")

      if is_input_table_set:
         st.success("テーブルが選択されました", icon="✅")
         try:
            # パラメータ化されたクエリを使用
            reference_query = "SELECT * FROM reference(?);"
            test_query = "SELECT * FROM reference(?) LIMIT 5;"
            query = test_query if test_mode else reference_query
            input_df = self.session.sql(query, params=(amedas_maker.input_table_ref,)).to_pandas()
         except Exception as e:
            st.error("テーブルデータの取得中にエラーが発生しました。")
            st.error(f"エラー内容: {str(e)}")
            return False

         amedas_maker.input_df = input_df
         st.write("注意 : 最初の100行のみ表示されます")
         st.write(amedas_maker.input_df[:100])
      else:
         st.info("テーブルを選択してください", icon="ℹ")

      return is_input_table_set

   def select_address_column(self, is_input_table_set: bool, input_df: pd.DataFrame) -> Tuple[bool, List[str]]:
      """住所データを含むカラムを選択するUIを表示する"""
      st.subheader("2️⃣ 住所データをもつカラムの選択")
      is_2_ok = False

      try:
         address_columns = [col for col in st.session_state.get('address_columns', []) if col in input_df.columns]
      except Exception as e:
         st.error("カラムの取得中にエラーが発生しました", icon="⛔")
         st.error(f"エラー内容: {str(e)}")
         return False, []

      if not is_input_table_set:
         st.info("1️⃣ を完了させてください", icon="ℹ")
      else:
         address_columns = st.multiselect("住所データのカラムを選択してください", \
            input_df.columns, default=address_columns)

         if not address_columns:
               st.error("住所データのカラムが選択されていません", icon="⛔")
         elif len(address_columns) > 1:
               st.error("住所データのカラムは1つだけ選択してください", icon="⛔")
         else:
               st.success("住所データのカラムが選択されました", icon="✅")
               is_2_ok = True
               st.session_state['address_columns'] = address_columns

      return is_2_ok, address_columns

   def select_date_range(self, is_2_ok: bool) -> Tuple[bool, Optional[pd.Timestamp], Optional[pd.Timestamp]]:
      """観測データの種類とデータ取得範囲を選択するUIを表示する"""
      st.subheader("3️⃣ 最寄りの観測点と気象データを取得")
      is_3_ok = False
      start_date: Optional[pd.Timestamp] = None
      end_date: Optional[pd.Timestamp] = None

      if not is_2_ok:
         st.info("2️⃣ を完了させてください", icon="ℹ")
         return is_3_ok, start_date, end_date
      else:
         try:
               # データ取得範囲の設定
               min_date_str, max_date_str = DataFetcher.get_min_max_date()
               min_date = pd.Timestamp(min_date_str)
               max_date = pd.Timestamp(max_date_str)
               # キーを使用して一意の日付入力ウィジェットを作成
               start_date = st.date_input("データ取得開始日",
                                          value=min_date,
                                          min_value=min_date,
                                          max_value=max_date,
                                          key="start_date_input")
               end_date = st.date_input("データ取得終了日",
                                       value=max_date,
                                       min_value=min_date,
                                       max_value=max_date,
                                       key="end_date_input")

               # 日付の妥当性チェック
               if start_date is not None and end_date is not None and \
                  pd.Timestamp(start_date) > pd.Timestamp(end_date):
                  st.error("開始日が終了日より後になっています。日付を修正してください。", icon="⛔")
                  return is_3_ok, None, None

               is_3_ok = True
               return is_3_ok, pd.Timestamp(start_date), pd.Timestamp(end_date)

         except Exception as e:
               st.error("データ選択中にエラーが発生しました。", icon="⛔")
               st.error(f"エラー内容: {str(e)}")
               return is_3_ok, None, None

   def execute_final_process(self, amedas_maker: 'AMeDASDataMaker', address_columns: List[str],
                                start_date: Optional[pd.Timestamp], end_date: Optional[pd.Timestamp]) -> None:
      """実行ボタンが押された際の処理"""
      if st.button("実行"):
         start_time: float = time.time()
         with st.spinner('実行中...お待ちください 🕒  \
            注意 : 実行中は実行ボタンに触れず、処理をやり直したい場合はページをリロードしてください 🔃'):
            try:
               # 日次データの取得
               amedas_maker.daily_df = amedas_maker.data_fetcher.get_daily_amedas()
               amedas_maker.daily_df['DATE'] = pd.to_datetime(amedas_maker.daily_df['DATE'])
               amedas_maker.daily_df = amedas_maker.daily_df[
                  (amedas_maker.daily_df['DATE'] >= pd.Timestamp(start_date)) &
                  (amedas_maker.daily_df['DATE'] <= pd.Timestamp(end_date))
               ]
               amedas_maker.daily_df['DATE'] = amedas_maker.daily_df['DATE'].dt.date

               # データ処理
               data_processor: DataProcessor = DataProcessor(amedas_maker.input_df, amedas_maker.observatory_df)
               result_df: pd.DataFrame = data_processor.process_input_data(address_columns[0])
               # 元のインデックスを保持するための列を追加
               amedas_maker.input_df['ORIGINAL_INDEX'] = amedas_maker.input_df.index

               # DataFrameを結合
               final_df: pd.DataFrame = pd.concat(
                  [amedas_maker.input_df.reset_index(drop=True),
                  result_df[['NEAREST_OBSERVATORY', 'MATCH_LEVEL']].reset_index(drop=True)],
                  axis=1
               )

               # マージ操作を行う
               final_df = final_df.merge(
                  amedas_maker.daily_df,
                  how='left',
                  left_on='NEAREST_OBSERVATORY',
                  right_on='OBSERVATORY_NAME'
               )

               # 不要な列を削除
               final_df = final_df.drop(columns=['CLEANSED_ADDRESS', 'OBSERVATORY_NAME'])

               # DATE列でソートしつつ元のインデックス順を保持する
               final_df = final_df.sort_values(['ORIGINAL_INDEX', 'DATE']).reset_index(drop=True)

               # インデックス列を削除
               final_df = final_df.drop(columns=['ORIGINAL_INDEX'])

               # テーブル作成とデータ保存、結果の表示
               st.success("データ取得完了！", icon="✅")
               amedas_maker.create_output_table(final_df)
               amedas_maker.save_to_output_table(final_df)
               st.write(f"##### 元データ + 最寄りの観測点 + 一致レベル + 気象データ({len(final_df)}行)")
               st.write("注意 : 最初の100行のみ表示しています")
               styled_df = amedas_maker.highlight_columns('NEAREST_OBSERVATORY', final_df[:100]).format(precision=1)
               st.dataframe(styled_df)
               st.write("##### 範囲別マッチング率")
               st.write(result_df['MATCH_LEVEL'].value_counts(normalize=True)
                        .mul(100)
                        .round(0)
                        .astype(int)
                        .astype(str) + '%')

               end_time: float = time.time()
               execution_time: int = int(end_time - start_time)
               st.write(f"##### 実行時間 : {execution_time} 秒")

               st.write("##### ※ 出力用テーブルに保存された全てのデータは、以下のクエリを使用して参照できます。")
               st.code("SELECT * FROM PODB_AMEDAS_RAINFALL_APP.CORE.WITH_AMEDAS", language="SQL")

            except Exception as e:
               st.error("データ処理中にエラーが発生しました。", icon="⛔")
               st.error(f"エラー内容: {str(e)}")

class AMeDASDataMaker:
   """AMeDASDataMakerクラス

   アプリケーションのメインクラス
   """

   def __init__(self, session: Session, input_table_ref: str) -> None:
      """AMeDASDataMakerのコンストラクタ。セッションや入力テーブル参照名を初期化し、必要なオブジェクトを生成

      :arg session: Snowflakeのセッションオブジェクト
      :arg input_table_ref: 参照する入力テーブルの名前
      """
      self.session = session  # Snowflakeのセッションオブジェクト
      self.input_table_ref: str = input_table_ref  # 入力テーブルの参照名
      self.input_df: pd.DataFrame = pd.DataFrame()  # 入力データフレームの初期化
      self.data_fetcher: DataFetcher = DataFetcher(session)  # データ取得用のDataFetcherオブジェクト
      self.ui_handler: UIHandler = UIHandler(session)  # UI操作用のUIHandlerオブジェクト
      self.daily_df: pd.DataFrame = pd.DataFrame()  # daily_dfの初期化
      try:
         self.observatory_df: pd.DataFrame = self.data_fetcher.get_daily_nearest_observatory()

      except SnowparkSQLException as e:
         st.error("観測所データの取得中にエラーが発生しました。", icon="⛔")
         st.error(f"エラー内容: {str(e)}")
         self.observatory_df = pd.DataFrame()

   def check_input_table_ref_existence(self) -> bool:
      """入力テーブルの参照が存在するかチェックする

      :return: テーブルが存在する場合True、そうでない場合False
      """
      try:
         ref_objects = permissions.get_reference_associations(self.input_table_ref)
         return len(ref_objects) > 0

      except Exception as e:
         st.error("入力テーブルの参照チェック中にエラーが発生しました。システム管理者に連絡してください。", icon="⛔")
         st.error(f"エラー内容: {str(e)}")
         return False

   def create_output_table(self, final_df: pd.DataFrame) -> None:
      """指定されたカラムに基づいて、新しいテーブルを作成する。

      :arg columns: 作成するテーブルのカラム名のリスト
      """
      try:
         # テーブルが存在するか確認
         table_exists_query = """
         SELECT COUNT(*)
         FROM INFORMATION_SCHEMA.TABLES
         WHERE TABLE_SCHEMA = 'CORE' AND TABLE_NAME = 'WITH_AMEDAS'
         """
         table_exists = self.session.sql(table_exists_query).collect()[0][0] > 0

         if table_exists:
               self.session.sql("GRANT DELETE ON TABLE CORE.WITH_AMEDAS TO APPLICATION ROLE app_public;").collect()
               # テーブルのデータをクリア（テーブルが存在することが前提）
               self.session.sql("DROP TABLE IF EXISTS CORE.WITH_AMEDAS").collect()
               st.warning("出力用スキーマにテーブルが既に存在していたため、クリアしました。", icon="⚠️")

         # SQL的な型のマッピング
         type_mapping = {
            'int64': 'NUMBER',
            'int32': 'NUMBER',
            'float64': 'FLOAT',
            'float32': 'FLOAT',
            'object': 'VARCHAR',
            'string': 'VARCHAR',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'timedelta[ns]': 'INTERVAL',
            'category': 'VARCHAR'
         }

         # 結果を保存するリスト
         sql_types = []

         # dtypes を final_df から取得
         dtypes = final_df.dtypes

         # 各列のデータ型を確認し、SQL型にマッピング
         for column in final_df.columns:
            dtype = dtypes[column]

            # datetime64[ns] の処理
            if 'datetime64' in str(dtype):
               if 'tz' in str(dtype):
                     sql_type = 'TIMESTAMP WITH TIME ZONE'
               else:
                     # 時刻が全て 00:00:00 なら DATE として扱う
                     if final_df[column].dt.time.eq(pd.Timestamp('00:00:00').time()).all():
                        sql_type = 'DATE'
                     else:
                        sql_type = 'TIMESTAMP'
            else:
               sql_type = type_mapping.get(str(dtype), 'VARCHAR')

            # 結果を追加
            sql_types.append((column, sql_type))

         # クエリのベース部分
         columns_sql = "CREATE TABLE IF NOT EXISTS CORE.WITH_AMEDAS ("

         # 各列とSQL型を追加
         for column, sql_type in sql_types:
            columns_sql += f"\"{column}\" {sql_type}, "

         # 最後のカンマを削除し、閉じ括弧を追加
         columns_sql = columns_sql.rstrip(", ") + ");"

         # テーブル作成と権限付与
         self.session.sql(columns_sql).collect()

         for privilege in ["DELETE", "SELECT", "INSERT"]:
            self.session.sql(
                  f"GRANT {privilege} ON TABLE CORE.WITH_AMEDAS TO APPLICATION ROLE app_public;"
            ).collect()
      except SnowparkSQLException as e:
         st.error("テーブル作成中にエラーが発生しました。権限が不足している可能性があります。", icon="⛔")
         st.error(f"エラー内容: {str(e)}")
      except Exception as e:
         st.error("予期せぬエラーが発生しました。システム管理者に連絡してください。", icon="⛔")
         st.error(f"エラー内容: {str(e)}")

   def save_to_output_table(self, df: pd.DataFrame) -> None:
      """指定されたDataFrameをSnowflakeのテーブルに保存する。

      :arg df: 保存するデータフレーム
      """
      try:
         temp_table = "TEMP_AMEDAS_DATA"

         # 一時テーブルを削除する
         self.session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()

         # DataFrameの内容を一時テーブルに保存する
         self.session.write_pandas(df, temp_table, overwrite=True)

         # 一時テーブルから本テーブルにデータを挿入する
         insert_query = """INSERT INTO CORE.WITH_AMEDAS SELECT * FROM TEMP_AMEDAS_DATA"""
         self.session.sql(insert_query).collect()

         st.success("データが出力用テーブルに保存されました。", icon="✅")

      except SnowparkSQLException as e:
         st.error("データ保存中にSQLエラーが発生しました。テーブル構造を確認してください。", icon="⛔")
         st.error(f"エラー内容: {str(e)}")
      except Exception as e:
         st.error("データ保存中に予期せぬエラーが発生しました。システム管理者に連絡してください。", icon="⛔")
         st.error(f"エラー内容: {str(e)}")

   def highlight_columns(self, col_name: str, df: pd.DataFrame) -> pd.io.formats.style.Styler:
      """指定されたカラムに背景色を適用する"""
      # NEAREST_OBSERVATORY列以降のセルに背景色を適用
      return df.style.applymap(lambda val: 'background-color: #ddffdd', subset=pd.IndexSlice[:, [col_name]])

   def run(self) -> None:
      """アプリのメイン処理を実行"""
      try:
         self.ui_handler.setup_page()

         # テーブル選択部分の処理(1️⃣)
         is_input_table_set = self.ui_handler.select_table(self)

         # 住所カラム選択部分の処理(2️⃣)
         is_2_ok, address_columns = self.ui_handler.select_address_column(is_input_table_set, self.input_df)

         # 日次データのタイプと期間の選択部分の処理(3️⃣)
         is_3_ok, start_date, end_date = self.ui_handler.select_date_range(is_2_ok)

         # 実行ボタン押下後の処理
         if is_3_ok:
            self.ui_handler.execute_final_process(self, address_columns, start_date, end_date)

      except Exception as e:
         st.error("アプリケーションの実行中に予期せぬエラーが発生しました。再試行してください。", icon="⛔")
         st.error(f"エラー内容: {str(e)}")

# アプリケーションの実行
if __name__ == "__main__":

   # ! 🔽の変数をTrueに設定すると、テストモードで実行されます
   test_mode = True
   # ! 🔼の変数をTrueに設定すると、テストモードで実行されます

   try:
      session = get_active_session()
      amedas_app = AMeDASDataMaker(session, "consumer_input_table")
      amedas_app.run()
   except Exception as e:
      st.error("アプリケーションの初期化中にエラーが発生しました。システム管理者に連絡してください。", icon="⛔")
      st.error(f"エラー内容: {str(e)}")
