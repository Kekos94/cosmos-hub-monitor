"""Read only Cosmos SDK TxRaw message URLs and declared coins locally.

Field numbers follow cosmos/tx/v1beta1/tx.proto and cosmos/base/v1beta1/coin.proto.
This is not a signature verifier or a decoder for message payloads.
"""

def varint(raw, offset):
    result = 0
    for index in range(10):
        if offset >= len(raw):
            raise ValueError('Truncated protobuf varint')
        value = raw[offset]
        offset += 1
        if index == 9 and value > 1:
            raise ValueError('Protobuf varint overflow')
        result |= (value & 127) << (index * 7)
        if value < 128:
            return result, offset
    raise ValueError('Invalid protobuf varint')


def fields(raw):
    offset = 0
    while offset < len(raw):
        tag, offset = varint(raw, offset)
        field, wire = tag >> 3, tag & 7
        if field == 0:
            raise ValueError('Invalid protobuf field zero')
        if wire == 0:
            value, offset = varint(raw, offset)
        elif wire in (1, 2, 5):
            if wire == 2:
                length, offset = varint(raw, offset)
            else:
                length = 8 if wire == 1 else 4
            if length > len(raw)-offset:
                raise ValueError('Truncated protobuf field')
            value = raw[offset:offset+length]
            offset += length
        else:
            raise ValueError('Unsupported protobuf wire type')
        yield field, wire, value


def repeated(raw, number):
    values = []
    for field, wire, value in fields(raw):
        if field == number:
            if wire != 2:
                raise ValueError('Expected protobuf bytes field')
            values.append(value)
    return values


def single(raw, number, *, required=False):
    values = repeated(raw, number)
    if len(values) > 1 or (required and not values):
        raise ValueError('Missing or duplicate protobuf field')
    return values[0] if values else b''


def decode_tx(raw):
    body = single(raw, 1, required=True)
    auth = single(raw, 2, required=True)
    messages = []
    for message in repeated(body, 1):
        url = single(message, 1, required=True).decode('utf-8')
        if not url.startswith('/'):
            raise ValueError('Invalid Cosmos message type URL')
        messages.append({'@type': url})
    if not messages:
        raise ValueError('Transaction has no message types')
    fee = single(auth, 2, required=True)
    amounts = []
    for coin in repeated(fee, 1):
        denom = single(coin, 1, required=True).decode('utf-8')
        amount = single(coin, 2, required=True).decode('utf-8')
        if not denom or not amount.isascii() or not amount.isdigit():
            raise ValueError('Invalid protobuf coin')
        amounts.append({'denom': denom, 'amount': amount})
    return {'body': {'messages': messages}, 'auth_info': {'fee': {'amount': amounts}}}
