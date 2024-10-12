/* ==============================================================================
  アプリケーションで使用するロールとスキーマを作成
==============================================================================*/

create Application role if not exists app_public;
CREATE OR ALTER VERSIONED SCHEMA CORE;

grant usage on schema CORE to Application role app_public;


/* ==============================================================================
  Streamlitの作成
==============================================================================*/
create streamlit if not exists CORE.AMeDAS_streamlit
from '/streamlit'
main_file = "main.py";

grant usage on streamlit CORE.AMeDAS_streamlit to application role app_public;

/* ==============================================================================
  リファレンス設定時にトリガーされるプロシージャ
  - システムコール呼び出しで、リファレンス設定や削除を行う
  - manifestのregister_callbackでこのコールバックを指定
  参考: https://docs.snowflake.com/en/developer-guide/native-apps/requesting-refs#create-a-callback-stored-procedure-for-a-reference
==============================================================================*/

CREATE or replace PROCEDURE CORE.REGISTER_SINGLE_REFERENCE(ref_name STRING, operation STRING, ref_or_alias STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
    BEGIN
      CASE (operation)
        WHEN 'ADD' THEN
          SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'CLEAR' THEN
          SELECT SYSTEM$REMOVE_ALL_REFERENCES(:ref_name);
      ELSE
        RETURN 'unknown operation: ' || operation;
      END CASE;
      RETURN NULL;
    END;
  $$;

GRANT USAGE ON PROCEDURE CORE.REGISTER_SINGLE_REFERENCE(STRING, STRING, STRING) TO APPLICATION ROLE app_public;

-- tableの読み取り権限を追加
CREATE OR REPLACE TABLE CORE.DAILY_AMEDAS AS SELECT * FROM PUBLIC.DAILY_AMEDAS;
CREATE OR REPLACE TABLE CORE.DAILY_OBSERVATORY_RAINFALL AS SELECT * FROM PUBLIC.DAILY_OBSERVATORY_RAINFALL;
GRANT SELECT ON TABLE CORE.DAILY_AMEDAS TO APPLICATION ROLE app_public;
GRANT SELECT ON TABLE CORE.DAILY_OBSERVATORY_RAINFALL TO APPLICATION ROLE app_public;