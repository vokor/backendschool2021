def prepare_couriers(data):
    prepared_data = []
    for courier in data['data']:
        prepared_data.append({'_id': courier['courier_id'],
                              'courier_type': courier['courier_type'],
                              'regions': courier['regions'],
                              'working_hours': courier['working_hours'],
                              'assigns': 0})
    return prepared_data


def prepare_orders(data):
    prepared_data = []
    for order in data['data']:
        prepared_data.append({'_id': order['order_id'],
                              'weight': order['weight'],
                              'region': order['region'],
                              'delivery_hours': order['delivery_hours'],
                              'status': 'not_assigned',
                              'courier_id': None,
                              'assign_time': None,
                              'complete_time': None})
    return prepared_data


def prepare_order(order_id, weight=3, region=3, delivery_hours=None, status='not_assigned',
                  courier_id=None, assign_time=None, complete_time=None):
    if delivery_hours is None:
        delivery_hours = []
    return {'_id': order_id,
            'weight': weight,
            'region': region,
            'delivery_hours': delivery_hours,
            'status': status,
            'courier_id': courier_id,
            'assign_time': assign_time,
            'complete_time': complete_time}
