def _operator(opstr):
    return {
        '$eq': '=',
        '$ne': '!=',                
        '$lt': '<',
        '$lte': '<=',                
        '$gt': '>',                
        '$gte': '>=',                
    }.get(opstr, '')
    
def _flatten(d):

    result = {}
    
    def _key(parent, key):
        if parent:
            return '{0}.{1}'.format(parent, key)
        return key
        
    def _f(d, parent=None):
        for key, val in d.items():
            keystr = _key(parent, key)
            if type(val) == dict:
                _f(val, keystr)
            else:
                result[keystr] = val

    _f(d)
    
    return result
            
class Mango:
    def __init__(self, query):
        self.query = query
        self.fields = []
        self.order = []
        self.discriminants = []
        
    def _fields(self):
        # Find the requested fields: they will form the SELECT a, b, c... part, which we 
        # need to extract from the json payload, apart from _id and _rev
        for field in self.query['fields']:
            if field in ['_id', '_rev']:
                self.fields.append(field)
            else:
                self.fields.append('json_extract(body, "$.{0}") AS {0}'.format(field)) # NOT injectable; see e.g....[so]
        
    def _order(self):
        # Optional sorting goes into ORDER BY x, y, x   
        # TODO: can rely on default direction, given array
        if 'sort' in self.query:
            for sorter in self.query['sort']:
                for (field, direction) in sorter.items():
                    if direction.upper() in ['ASC', 'DESC']:
                        self.order.append('{0} {1}'.format(field, direction))
            
    def _discriminant(self):
        for (field, value) in self.query['selector'].items():
            if type(value) != dict: # scalar TODO: not sufficient; can also be array
                # If part of the requested fields, we don't extract from json, as should have been aliased already.
                if field in self.fields:
                    self.discriminants.append(['{0}=?'.format(field), value])
                else: # Discriminant not requested
                    self.discriminants.append(['json_extract(body, "$.{0}")=?'.format(field), value])
            else: # A dict -- either a sub-field query or an operator. Or both.
                # Sub-field as json object:
                #
                # "imdb": {
                #     "rating": 8
                # }
                # 
                # Flatten to "imdb.rating": 8
                # 
                # The operator case:
                #
                # "imdb": {
                #     "rating": {
                #         "$gt": 8
                #     }
                # }
                #
                # This will need to be extracted from the json.
                for (fkey, fval) in _flatten({field: value}).items():
                    if '.$' in fkey:
                        key_components = fkey.split('.')
                        op = key_components[-1]
                        if _operator(op):
                            self.discriminants.append(['json_extract(body, "$.{0}"){1}?'.format('.'.join(key_components[:-1]), _operator(op)), fval])
                        else:
                            raise SovocError('Bad selector syntax')
                    else:
                        self.discriminants.append(['json_extract(body, "$.{0}")=?'.format(fkey), fval])
                        
    def statement(self):
        fieldstr = ''
        self._fields()
        if self.fields:
            fieldstr = ', '.join(self.fields)
             
        # Optional sorting goes into ORDER BY x, y, x   
        orderstr = ''
        self._order()
        if self.order:
            orderstr = ' ORDER BY {0}'.format(','.join(self.order))
            
        # The 'selector' is the discriminant, i.e. the WHERE i, j, k bit of the statement
        wherestr = ''
        self._discriminant()
        if self.discriminants:
            wherestr = ' WHERE {0}'.format(' AND '.join([term[0] for term in self.discriminants]))
    
        statement = 'SELECT {0} FROM documents{1}{2}'.format(fieldstr, wherestr, orderstr)
        
        print(statement)
        
        values = [term[1] for term in self.discriminants]
        
        return statement, values