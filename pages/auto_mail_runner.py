from warehouse_dashboard import *

send_expiry_alerts(expiry_df, tx_df, 60)
send_expiry_alerts(expiry_df, tx_df, 30)
send_expiry_alerts(expiry_df, tx_df, 10)

send_negative_stock_mail(tx_df)
send_available_stock_snapshot(tx_df)
