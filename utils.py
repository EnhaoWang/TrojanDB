import os

interface='TrojanDB>> ' 

def print_error_message(type, missing_name=None):
	error_messages={
		'syntax': 'Error: There is a syntax error in your input. Please check and enter again.',
		'missing_table': f'Error: Table "{missing_name}" does not exist!',
		'missing_column': f'Error: Column "{missing_name}" does not exist!',
		'not_match': 'Error: Number of columns and values does not match!',	
		'and_or': 'Error: Sorry, this database system can only support at most one "and" / "or" after "where".',
		'group0_aggr1': 'Error: Aggregate function must be used with "cateby"!',
		'group1_aggr0': 'Error: "cateby" must be used with aggregate function!',
		'null': 'Error: It\'s not a valid comparison on "NULL" value.'
	}

	print(interface + error_messages[type])

def remove_temp_files():
	try: os.remove('result.csv')
	except: pass
	try: os.remove('temp.csv')
	except: pass

def get_path(table_name):
	return f'./dataset/{table_name}.csv'

chunksize=100
