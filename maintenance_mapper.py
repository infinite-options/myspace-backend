def mapMaintenanceStatusByUserType(response, user_type):
    if user_type == 'OWNER':
        return mapMaintenanceStatusForOwner(response)
    elif user_type == 'TENANT':
        return mapMaintenanceStatusForTenant(response)
    elif user_type == 'MANAGEMENT':
        return mapMaintenanceStatusForPropertyManager(response)
    elif user_type == 'MAINTENANCE':
        return mapMaintenanceStatusForMaintenancePerson(response)
    return response


def mapMaintenanceStatusForOwner(response):
    status_colors = {
        'NEW REQUEST': '#A52A2A',
        'INFO REQUESTED': '#C06A6A',
        'QUOTES REQUESTED': '#D29494',
        'PROCESSING': '#3D5CAC',
        'SCHEDULED': '#3D5CAC',
        'COMPLETED': '#3D5CAC',
    }

    mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in status_colors.items()}
    for record in response['result']:
        if record['maintenance_request_status'] == 'NEW':
            mapped_items['NEW REQUEST']['maintenance_items'].append(record)
        elif record['maintenance_request_status'] == 'INFO':
            mapped_items['INFO REQUESTED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'REQUESTED':
            mapped_items['QUOTES REQUESTED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'SENT' or record['quote_status'] == 'WITHDRAWN' \
                or record['quote_status'] == 'REFUSED' or record['quote_status'] == 'REJECTED' \
                or record['quote_status'] == 'ACCEPTED' or record['quote_status'] == 'SCHEDULE':
            mapped_items['PROCESSING']['maintenance_items'].append(record)
        elif record['quote_status'] == 'SCHEDULED' or record['quote_status'] == 'RESCHEDULED':
            mapped_items['SCHEDULED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'FINISHED' or record['quote_status'] == 'COMPLETED':
            mapped_items['COMPLETED']['maintenance_items'].append(record)

    response['result'] = mapped_items
    return response


def mapMaintenanceStatusForTenant(response):
    status_colors = {
        'NEW REQUEST': '#A52A2A',
        'INFO REQUESTED': '#C06A6A',
        'PROCESSING': '#3D5CAC',
        'SCHEDULED': '#3D5CAC',
        'COMPLETED': '#3D5CAC',
    }

    mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in status_colors.items()}
    for record in response['result']:
        if record['maintenance_request_status'] == 'NEW':
            mapped_items['NEW REQUEST']['maintenance_items'].append(record)
        elif record['maintenance_request_status'] == 'INFO':
            mapped_items['INFO REQUESTED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'SENT' or record['quote_status'] == 'WITHDRAWN' \
                or record['quote_status'] == 'REFUSED' or record['quote_status'] == 'REJECTED' \
                or record['quote_status'] == 'ACCEPTED' or record['quote_status'] == 'SCHEDULE':
            mapped_items['PROCESSING']['maintenance_items'].append(record)
        elif record['quote_status'] == 'SCHEDULED' or record['quote_status'] == 'RESCHEDULE':
            mapped_items['SCHEDULED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'WITHDRAWN' or record['quote_status'] == 'FINISHED' or record['quote_status'] == 'COMPLETED':
            mapped_items['COMPLETED']['maintenance_items'].append(record)

    response['result'] = mapped_items
    return response


def mapMaintenanceStatusForPropertyManager(response):
    status_colors = {
        'NEW REQUEST': '#A52A2A',
        'QUOTES REQUESTED': '#C06A6A',
        'QUOTES ACCEPTED': '#D29494',
        'SCHEDULED': '#9EAED6',
        'COMPLETED': '#778DC5',
        'PAID': '#3D5CAC',
    }

    mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in status_colors.items()}
    for record in response['result']:
        if record['maintenance_request_status'] == 'NEW' or record['maintenance_request_status'] == 'INFO':
            mapped_items['NEW REQUEST']['maintenance_items'].append(record)
        elif record['quote_status'] == 'REQUESTED' or record['quote_status'] == 'SENT' \
                or record['quote_status'] == 'WITHDRAWN' or record['quote_status'] == 'REFUSED' \
                or record['quote_status'] == 'REJECTED':
            mapped_items['QUOTES REQUESTED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'ACCEPTED' or record['quote_status'] == 'SCHEDULE':
            mapped_items['QUOTES ACCEPTED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'SCHEDULED' or record['quote_status'] == 'RESCHEDULE':
            mapped_items['SCHEDULED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'WITHDRAWN' or record['quote_status'] == 'FINISHED':
            mapped_items['COMPLETED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'COMPLETED':
            mapped_items['PAID']['maintenance_items'].append(record)

    response['result'] = mapped_items
    return response


def mapMaintenanceStatusForMaintenancePerson(response):
    status_colors = {
        'REQUESTED': '#DB9687',
        'SUBMITTED': '#D4A387',
        'ACCEPTED': '#BAAC7A',
        'SCHEDULED': '#959A76',
        'FINISHED': '#598A96',
        'PAID': '#497290',
    }

    mapped_items = {k: {'maintenance_color': v, 'maintenance_items': []} for k, v in status_colors.items()}
    for record in response['result']:
        if record['quote_status'] == 'REQUESTED':
            mapped_items['REQUESTED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'SENT' or record['quote_status'] == 'WITHDRAWN' \
                or record['quote_status'] == 'REFUSED' or record['quote_status'] == 'REJECTED':
            mapped_items['SUBMITTED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'ACCEPTED' or record['quote_status'] == 'SCHEDULE':
            mapped_items['ACCEPTED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'SCHEDULED' or record['quote_status'] == 'RESCHEDULE':
            mapped_items['SCHEDULED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'WITHDRAWN':
            mapped_items['SUBMITTED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'FINISHED':
            mapped_items['FINISHED']['maintenance_items'].append(record)
        elif record['quote_status'] == 'COMPLETED':
            mapped_items['PAID']['maintenance_items'].append(record)

    response['result'] = mapped_items
    return response
