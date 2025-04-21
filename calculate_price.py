def inclusive_price(item):
    
    rrp = float(item.get('rrp', 0))
    discount = float(item.get('discount', 0))
    weight = float(item.get('weight', 0))
    per_item = 0.08
    quantity = 1
    ebay_fpf = 9.9
    dropship_fee = 0.7
    pro_margin = 10

    cp = rrp - (rrp * (discount / 100))

    if weight > 1600:
        pfee = 2.57
    else:
        pfee = 2.22
        
    item_fee = per_item * quantity

    base_cost = cp + item_fee + pfee + dropship_fee

    ebay_fee = (base_cost * (ebay_fpf / 100)) + item_fee

    final_price = (base_cost + ebay_fee) * (1 + pro_margin / 100)
    
    return final_price
