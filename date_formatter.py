list_data_detail1 = []
for item in list_data_detail:
	if str(item["PAY_DATE"]) is not None and str(item["PAY_DATE"]) != "":
		item_pay_date = datetime.strptime(str(item["PAY_DATE"]),"%Y-%m-%d").strftime("%m%d/%Y")
	else:
		item_pay_date = None
	if str(item["POST_DATE"]) is not None and str(item["POST_DATE"]) != "":
		item_post_date = datetime.strptime(str(item["POST_DATE"]),"%Y-%m-%d").strftime("%m%d/%Y")
	else:
		item_post_date = None
	new_item = item
	new_item["FORMATTED_PAY_DATE"] = item_pay_date
	new_item["FORMATTED_POST_DATE"] = item_post_date
	list_data_detail1.append(new_item)
	
