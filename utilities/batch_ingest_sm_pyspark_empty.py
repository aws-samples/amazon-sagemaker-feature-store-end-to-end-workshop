
def transform_query(fg_name: str) -> str:
    return f'''
        SELECT *
        FROM {fg_name}
        '''
