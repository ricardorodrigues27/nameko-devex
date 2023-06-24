import json

from marshmallow import ValidationError
from nameko import config
from nameko.exceptions import BadRequest
from nameko.rpc import RpcProxy
from werkzeug import Response

from gateway.entrypoints import http
from gateway.exceptions import OrderNotFound, ProductNotFound
from gateway.schemas import CreateOrderSchema, GetOrderSchema, ListOrdersSchema, ProductSchema


class GatewayService(object):
    """
    Service acts as a gateway to other services over http.
    """

    name = 'gateway'

    orders_rpc = RpcProxy('orders')
    products_rpc = RpcProxy('products')

    @http(
        "GET", "/products/<string:product_id>",
        expected_exceptions=ProductNotFound
    )
    def get_product(self, request, product_id):
        """Gets product by `product_id`
        """
        product = self.products_rpc.get(product_id)
        return Response(
            ProductSchema().dumps(product).data,
            mimetype='application/json'
        )

    @http(
        "POST", "/products",
        expected_exceptions=(ValidationError, BadRequest)
    )
    def create_product(self, request):
        """Create a new product - product data is posted as json

        Example request ::

            {
                "id": "the_odyssey",
                "title": "The Odyssey",
                "passenger_capacity": 101,
                "maximum_speed": 5,
                "in_stock": 10
            }


        The response contains the new product ID in a json document ::

            {"id": "the_odyssey"}

        """

        schema = ProductSchema(strict=True)

        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.
            product_data = schema.loads(request.get_data(as_text=True)).data
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))

        # Create the product
        product = self.products_rpc.create(product_data)
        return Response(
            json.dumps({'id': product['id']}), mimetype='application/json'
        )

    @http(
        "DELETE", "/products/<string:product_id>",
        expected_exceptions=(ProductNotFound, BadRequest)
    )
    def delete_product(self, request, product_id):
        """Delete product by `product_id`

        The response contains the deleted product ID in a json document ::

            {"id": "the_odyssey"}

        """

        # Check if there is any Order related to the Product
        order_with_product_id = self.orders_rpc.get_order_by_product_id(product_id)
        if order_with_product_id is None:
            self.products_rpc.delete(product_id)
        else:
            raise BadRequest("Product with Order can not be deleted")

        return Response(
            json.dumps({'id': product_id}), mimetype='application/json'
        )

    @http("GET", "/orders")
    def list_orders(self, request):
        """List of orders with pagination.

        Enhances the order details with full product details from the
        products-service.

        You can use query parameter to fetch orders with pagination:

            page [integer]
            Page number of the results to fetch.
            Default: 1

            page_size [integer]
            The number of results per page (max 100).
            Default: 30

        Example:

            /orders?page=2&page_size=60

        The response contains the pagination info and the orders in a json document ::

            {
                "page": 1,
                "page_size": 30,
                "total": 100,
                "items": [Order]
            }
        """
        page = request.args.get('page', default=1, type=int)
        page_size = request.args.get('page_size', default=30, type=int)

        if page_size > 100:
            page_size = 100

        list_orders_data = self._list_orders(page, page_size)

        return Response(
            ListOrdersSchema().dumps(list_orders_data).data,
            mimetype='application/json'
        )
    
    def _list_orders(self, page, page_size):
        # Retrieve order data from the orders service using pagination
        # Note - the response contains additional information for pagination
        # Available keys: [page, page_size, total, items]
        result = self.orders_rpc.list_orders(page=page, page_size=page_size)

        if len(result['items']) > 0:
            # Filter and preload products to fill in order_details data
            orders_product_ids = self._extract_product_ids_from_orders(result['items'])
            product_map = {prod['id']: prod for prod in self._list_products(orders_product_ids)}

            for order in result['items']:
                order.update(self._fill_order_details_with_product(order, product_map))

        return result
    
    def _extract_product_ids_from_orders(self, orders):
        all_product_ids = []
        for order in orders:
            order_product_ids = self._extract_product_ids_from_order(order)
            all_product_ids.extend(order_product_ids)

        return all_product_ids
    
    def _extract_product_ids_from_order(self, order):
        return [order_detail['product_id'] for order_detail in order['order_details']]

    def _list_products(self, product_ids):
        return self.products_rpc.list(product_ids=product_ids)
    
    def _fill_order_details_with_product(self, order, product_map):
        # get the configured image root
        image_root = config['PRODUCT_IMAGE_ROOT']

        # Enhance order details with product and image details.
        for item in order['order_details']:
            product_id = item['product_id']

            item['product'] = product_map[product_id]
            # Construct an image url.
            item['image'] = '{}/{}.jpg'.format(image_root, product_id)

        return order

    @http("GET", "/orders/<int:order_id>", expected_exceptions=OrderNotFound)
    def get_order(self, request, order_id):
        """Gets the order details for the order given by `order_id`.

        Enhances the order details with full product details from the
        products-service.
        """
        order = self._get_order(order_id)
        return Response(
            GetOrderSchema().dumps(order).data,
            mimetype='application/json'
        )

    def _get_order(self, order_id):
        # Retrieve order data from the orders service.
        # Note - this may raise a remote exception that has been mapped to
        # raise``OrderNotFound``
        order = self.orders_rpc.get_order(order_id)

        # Filter and preload products to fill in order_details data
        order_product_ids = self._extract_product_ids_from_order(order)
        product_map = {prod['id']: prod for prod in self._list_products(order_product_ids)}

        return self._fill_order_details_with_product(order, product_map)

    @http(
        "POST", "/orders",
        expected_exceptions=(ValidationError, ProductNotFound, BadRequest)
    )
    def create_order(self, request):
        """Create a new order - order data is posted as json

        Example request ::

            {
                "order_details": [
                    {
                        "product_id": "the_odyssey",
                        "price": "99.99",
                        "quantity": 1
                    },
                    {
                        "price": "5.99",
                        "product_id": "the_enigma",
                        "quantity": 2
                    },
                ]
            }


        The response contains the new order ID in a json document ::

            {"id": 1234}

        """

        schema = CreateOrderSchema(strict=True)

        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.
            order_data = schema.loads(request.get_data(as_text=True)).data
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))
        
        # Note - this may raise `ProductNotFound`
        order_created = self._create_order(order_data)

        return Response(json.dumps({'id': order_created['id']}), mimetype='application/json')

    def _create_order(self, order_data):
        # Filter and preload products from product service:
        order_product_ids = self._extract_product_ids_from_order(order_data)
        found_product_ids = {prod['id'] for prod in self._list_products(order_product_ids)}
        
        # Check if order product ids are valid
        for item in order_data['order_details']:
            if item['product_id'] not in found_product_ids:
                raise ProductNotFound(
                    "Product Id {}".format(item['product_id'])
                )

        # Call orders-service to create the order.
        # Dump the data through the schema to ensure the values are serialized
        # correctly.
        serialized_data = CreateOrderSchema().dump(order_data).data
        return self.orders_rpc.create_order(serialized_data['order_details'])
