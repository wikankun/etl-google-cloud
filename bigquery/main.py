from create_bq_table import create_table, schemaTempKeyword, \
    schemaConvertedKeyword, schemaTopKeyword, schemaTransaction

temp_search_history_table_id = "blank-space-315611.search_history.temp_search_history"
converted_search_history_table_id = "blank-space-315611.search_history.converted_search_history"
top_search_history_table_id = "blank-space-315611.search_history.top_search_history"
transaction_table_id = "blank-space-315611.transactions.transaction"

if __name__ == "__main__":
    create_table(temp_search_history_table_id, schemaTempKeyword)
    create_table(converted_search_history_table_id, schemaConvertedKeyword)
    create_table(top_search_history_table_id, schemaTopKeyword)
    create_table(transaction_table_id, schemaTransaction)
