```mermaid
    classDiagram
    AMeDASDataMaker --> DataFetcher
    AMeDASDataMaker --> UIHandler
    AMeDASDataMaker --> DataProcessor
    
    class AMeDASDataMaker{
      +run()
      +check_input_table_ref_existence()
    }
    
    class DataFetcher{
      +get_daily_nearest_observatory()
      +get_daily_amedas()
    }
    
    class DataProcessor{
      +process_input_data()
      -replace_old_kanji()
      -replace_patterns()
      -normalize_address()
      -find_matching_row()
      -join_dfs()
    }
    
    class UIHandler{
      +setup_page()
      +select_table()
      +select_address_column()
      +select_observatory_data_type()
      +select_daily_data_type_and_date_range()
    }

    AMeDASDataMaker ..> permissions
````