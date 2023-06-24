import pytest

from mock import call
from nameko.exceptions import RemoteError

from orders.models import Order, OrderDetail
from orders.schemas import OrderSchema, OrderDetailSchema


@pytest.fixture
def order(db_session):
    order = Order()
    db_session.add(order)
    db_session.commit()
    return order


@pytest.fixture
def order_details(db_session, order):
    db_session.add_all([
        OrderDetail(
            order=order, product_id="the_odyssey", price=99.51, quantity=1
        ),
        OrderDetail(
            order=order, product_id="the_enigma", price=30.99, quantity=8
        )
    ])
    db_session.commit()
    return order_details


def test_get_order(orders_rpc, order):
    response = orders_rpc.get_order(order.id)
    assert response['id'] == order.id


@pytest.mark.usefixtures('db_session')
def test_will_raise_when_order_not_found(orders_rpc):
    with pytest.raises(RemoteError) as err:
        orders_rpc.get_order(1)
    assert err.value.value == 'Order with id 1 not found'

@pytest.mark.usefixtures('order_details')
def test_get_order_by_product_id(orders_rpc, order):
    order_payload = OrderSchema().dump(order).data
    first_product_id = order_payload['order_details'][0]['product_id']

    response = orders_rpc.get_order_by_product_id(first_product_id)
    assert response['id'] == order.id

def test_list_orders(orders_rpc, order):
    response = orders_rpc.list_orders()
    assert response['items'][0]['id'] == order.id

    # Check response default pagination values
    assert response['page'] == 1
    assert response['page_size'] == 30
    assert response['total'] == 1

def test_list_orders_with_custom_pagination(orders_rpc, order):
    response = orders_rpc.list_orders(page=2, page_size=3)
    assert response['page'] == 2
    assert response['page_size'] == 3
    assert response['total'] == 1


@pytest.mark.usefixtures('db_session')
def test_can_create_order(orders_service, orders_rpc):
    order_details = [
        {
            'product_id': "the_odyssey",
            'price': 99.99,
            'quantity': 1
        },
        {
            'product_id': "the_enigma",
            'price': 5.99,
            'quantity': 8
        }
    ]
    new_order = orders_rpc.create_order(
        OrderDetailSchema(many=True).dump(order_details).data
    )
    assert new_order['id'] > 0
    assert len(new_order['order_details']) == len(order_details)
    assert [call(
        'order_created', {'order': {
            'id': 1,
            'order_details': [
                {
                    'price': '99.99',
                    'product_id': "the_odyssey",
                    'id': 1,
                    'quantity': 1
                },
                {
                    'price': '5.99',
                    'product_id': "the_enigma",
                    'id': 2,
                    'quantity': 8
                }
            ]}}
    )] == orders_service.event_dispatcher.call_args_list


@pytest.mark.usefixtures('db_session', 'order_details')
def test_can_update_order(orders_rpc, order):
    order_payload = OrderSchema().dump(order).data
    for order_detail in order_payload['order_details']:
        order_detail['quantity'] += 1

    updated_order = orders_rpc.update_order(order_payload)

    assert updated_order['order_details'] == order_payload['order_details']


def test_can_delete_order(orders_rpc, order, db_session):
    orders_rpc.delete_order(order.id)
    assert not db_session.query(Order).filter_by(id=order.id).count()
