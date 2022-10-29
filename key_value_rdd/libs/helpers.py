def show_pair(rdd):
    '''
    Prints rdd key value pairs
    '''
    for key, value in rdd.collect():
        print(f'{key, value}')