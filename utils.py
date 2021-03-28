def split_orders(orders_data, working_hours):
    av_orders = []
    un_orders = []
    for order in orders_data:
        find = False
        for st2, fn2 in order['delivery_hours']:
            for st1, fn1 in working_hours:
                if st2 <= st1 and fn1 <= fn2:
                    av_orders.append(order)
                    find = True
                    break
            if find:
                break
        if not find:
            un_orders.append(order['_id'])
    return av_orders, un_orders
