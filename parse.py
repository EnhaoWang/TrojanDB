import re
import os
from utils import interface, print_error_message, remove_temp_files, get_path
from funcs import insert, update, delete, join, filter, group_aggr, projection, order, print_query_result

# regular expressions
start_cmd=r'^\s*'
end_cmd=r'\s*;\s*$'
comma_sep=r'\s*\S+(?:\s*,\s*\S+?)*\s*' # column1, column2
table_re=r'\s*\w+\s*'
column_re=r'\s*\w+\s*'
value_re=r'\s*[^,]+?\s*'
operator_re=r'(?:[><]=?|!=|=)'
set_re=rf'{column_re}={value_re}(?:\s+and{column_re}={value_re})*'
where_re=rf'{column_re}{operator_re}(?:{column_re}|{value_re})(?:\s+(?:and|or)\s+{column_re}{operator_re}(?:{column_re}|{value_re}))*'
table_column_re=rf'(?:{column_re}|{table_re}\.{column_re})'
find_where_re=rf'{table_column_re}{operator_re}(?:{table_column_re}|{value_re})(?:\s+(?:and|or)\s+{table_column_re}{operator_re}(?:{table_column_re}|{value_re}))*'

def insert_cmd(cmd):
	# cases:
	# addto table1 (column1 $and column2  ) values (value1 $and value2);

	# check the syntax
	if not re.findall(rf'{start_cmd}addto\s+{table_re}\({column_re}(?:\s+\$and\s+{column_re})*\)\s+values\s*\({value_re}(?:\s+\$and\s+{value_re})*\){end_cmd}', cmd, re.IGNORECASE):
		remove_temp_files()
		print_error_message('syntax')
		return

	table=re.findall(rf'addto\s+({table_re}\({column_re}(?:\s+\$and\s+{column_re})*\))\s+values', cmd, re.IGNORECASE)[0].strip() # result: table1(column1, column2)
	table_name=re.findall(rf'({table_re})\(', table)[0].strip() # result: table1
	table_col=re.findall(rf'\(({column_re}(?:\s+\$and\s+{column_re})*)\)', table)[0].split('$and') # result: ['column1', 'column2']
	# remove spaces around the column names
	for i in range(len(table_col)):
	    table_col[i]=table_col[i].strip()

	file_path=get_path(table_name)
	if not os.path.exists(file_path): # no table file found
		remove_temp_files()
		print_error_message('missing_table', table_name)
		return

	values=re.findall(rf'values\s*\(({value_re}(?:\s+\$and\s+{value_re})*)\){end_cmd}', cmd)[0].split('$and') # result: ['value1', 'value2']
	# remove spaces around the values
	for i in range(len(values)):
	    values[i]=values[i].strip()

	# e.g. inputted 2 columns but 3 values
	if len(values)!=len(table_col):
		remove_temp_files()
		print_error_message('not_match')
		return

	new_values=list(zip(table_col, values)) # result: [['column1', 'value1'], ['column2', 'value2']]

	# insert values
	insert(file_path, new_values)

def update_cmd(cmd):
	# cases:
	# renew table1 set column1=value1;
	# renew table1 set column1=value1 where column2=value2;
	# renew table1 set column1=value1 and column2=value2 where column3=value3 and column4=value4;
	# renew table1 set column1=value1 and column2=value2 where column3=value3 or column4=value4;

	# check the syntax
	if not re.findall(rf'{start_cmd}renew\s+{table_re}\s+set\s+{set_re}(?:\s+where\s+{where_re})?{end_cmd}', cmd, re.IGNORECASE):
		remove_temp_files()
		print_error_message('syntax')
		return

	table_name=re.findall(rf'renew\s+({table_re})\s+set', cmd, re.IGNORECASE)[0].strip()
	file_path=get_path(table_name)
	if not os.path.exists(file_path):
		remove_temp_files()
		print_error_message('missing_table', table_name)
		return

	set_argv=re.findall(rf'set\s+({set_re})\s+where', cmd, re.IGNORECASE)[0].strip() # result: column1=value1 and column2=value2
	new_values=re.split(r'(?i)and', set_argv) # result: ['column1=value1', 'column2=value2']
	for i in range(len(new_values)):
		column, value=new_values[i].split('=')
		column=column.strip() # result: column1
		value=value.strip() # result: value1
		new_values[i]=[column, value]
		# new_values: [['column1', 'value1'], ['column2', 'value2']]

	where_argv=re.findall(rf'where\s+({where_re}){end_cmd}', cmd, re.IGNORECASE) # result: column3=value3 and/or column4=value4
	if where_argv:
		where_argv=where_argv[0].strip()
		# only one and/or is supported
		if where_argv.lower().count(' and ')+where_argv.lower().count(' or ')>1:
			remove_temp_files()
			print_error_message('and_or')
			return
		if ' or ' in where_argv.lower():
			logic='or'
			conditions=re.split(r'(?i) or ', where_argv) # result: ['column1>value1', 'column2!=value2']
		else:
			logic='and' 
			conditions=re.split(r'(?i) and ', where_argv) # result: ['column1>value1', 'column2!=value2']
		for i in range(len(conditions)):
			column, operator, value=re.findall(rf'^({column_re})({operator_re})({column_re}|{value_re})$', conditions[i])[0]
			column=column.strip() # result: column1
			operator=operator.strip() # result: >
			value=value.strip() # result: value1
			conditions[i]=[column, operator, value]
			# conditions: [['column1', '>', value1'], ['column2', '!=', 'value2']]

		update(file_path, new_values, conditions, logic)

	else:
		update(file_path, new_values)



def delete_cmd(cmd):
	# remove table1 where column1>value1 and column2!=value2;
	# remove table1;
	
	# check the syntax
	if not re.findall(rf'{start_cmd}remove\s+{table_re}(?:\s+where\s+{where_re})?{end_cmd}', cmd, re.IGNORECASE):
		remove_temp_files()
		print_error_message('syntax')
		return

	table_name=re.findall(rf'remove\s+({table_re})\s+where', cmd, re.IGNORECASE)[0].strip()
	file_path=get_path(table_name)
	if not os.path.exists(file_path): # no table file found
		remove_temp_files()
		print_error_message('missing_table', table_name)
		return

	where_argv=re.findall(rf'where\s+({where_re}){end_cmd}', cmd, re.IGNORECASE) # result: column3=value3 and/or column4=value4
	if where_argv:
		where_argv=where_argv[0].strip()
		# only one and/or is supported
		if where_argv.lower().count(' and ')+where_argv.lower().count(' or ')>1:
			remove_temp_files()
			print_error_message('and_or')
			return
		if ' or ' in where_argv.lower():
			logic='or'
			conditions=re.split(r'(?i) or ', where_argv) # result: ['column1>value1', 'column2!=value2']
		else:
			logic='and' 
			conditions=re.split(r'(?i) and ', where_argv) # result: ['column1>value1', 'column2!=value2']
		for i in range(len(conditions)):
			column, operator, value=re.findall(rf'^({column_re})({operator_re})({column_re}|{value_re})$', conditions[i])[0]
			column=column.strip() # result: column1
			operator=operator.strip() # result: >
			value=value.strip() # result: value1
			conditions[i]=[column, operator, value]
			# conditions: [['column1', '>', value1'], ['column2', '!=', 'value2']]
		delete(file_path, conditions, logic)
	else:
		delete(file_path)

		
	
	
def find_cmd(cmd):
	# cases:
	# find * in table1;
	# find column1 in table1 where column2 > 25; (>, <, =)
	# find table1.column3, table2.column4 in table1, table2 where table1.column1=table2.column2;
	# find column1, sum(column2) in table1 cateby column1;
	# find column1, sum(column2) in table1 where column3>20 cateby column1;
	# find column1 in table1 sortby column3;
	# find column1 in table1 where column2>20 sortby column3 descend;

	# check the syntax
	if not re.findall(
	    rf'{start_cmd}find\s+.+\s+'
	    rf'in\s+{table_re}(?:,{table_re})*'
	    rf'(?:\s+where\s+{find_where_re})?'
	    rf'(?:\s+cateby\s+{table_column_re})?'
	    rf'(?:\s+sortby\s+{table_column_re}(?:\s+descend)?)?{end_cmd}',
	    cmd, re.IGNORECASE):
		remove_temp_files()
		print_error_message('syntax')
		return
	
	tables=re.findall(rf'in\s+({table_re}(?:,{table_re})*)(?:\swhere|\scateby|\ssortby|{end_cmd})', cmd, re.IGNORECASE)[0].split(',')
	files=[]
	for i in range(len(tables)):
		table_name=tables[i].strip()
		file_path=get_path(table_name)
		if not os.path.exists(file_path): # no table file found
			remove_temp_files()
			print_error_message('missing_table', table_name)
			return
		files.append(file_path)

	input_file=files[0]

	# join
	if len(files)!=1:
		for j in range(1,len(files)):
			join(input_file, files[j])
			input_file='result.csv'

	# filter 
	where_argv=re.findall(rf'where\s+({find_where_re})(?:\scateby|\ssortby|{end_cmd})', cmd, re.IGNORECASE) # result: column3=value3 and/or column4=value4
	if where_argv:
		# only one and/or is supported
		where_argv=where_argv[0].strip()
		if where_argv.lower().count(' and ')+where_argv.lower().count(' or ')>1:
			remove_temp_files()
			print_error_message('and_or')
			return
		if ' or ' in where_argv.lower():
			logic='or'
			conditions=re.split(r'(?i) or ', where_argv) # result: ['column1>value1', 'column2!=value2']
		else:
			logic='and' 
			conditions=re.split(r'(?i) and ', where_argv) # result: ['column1>value1', 'column2!=value2']
		for i in range(len(conditions)):
			column, operator, value=re.findall(rf'^({table_column_re})({operator_re})({table_column_re}|{value_re})$', conditions[i])[0]
			column=column.strip() # result: column1
			operator=operator.strip() # result: >
			value=value.strip() # result: value1
			conditions[i]=[column, operator, value]
			# conditions: [['column1', '>', value1'], ['column2', '!=', 'value2']]
		func_result=filter(input_file, conditions, logic) 
		if func_result==0:
			return
		input_file='result.csv'

	# categorize
	cateby_argv=re.findall(rf'cateby\s+({table_column_re})(?:\ssortby|{end_cmd})', cmd, re.IGNORECASE)
	proj_col=[]
	if cateby_argv:
		# find aggregation function from 'find'
		find_argv=re.findall(rf'find\s+(.+)\s+in', cmd, re.IGNORECASE)[0].strip()
		find_col=find_argv.split(',')
		aggr_col=''		
		for col in find_col:
			col=col.strip()
			if re.findall(rf'(?:sum|count|avg|max|min)\s*\({table_column_re}\)', col, re.IGNORECASE) or \
			   re.findall(r'count\s*\(\s*\*\s*\)', col, re.IGNORECASE):
				if aggr_col!='':
					remove_temp_files()
					print_error_message('syntax')
					return
				aggr_func=re.findall(rf'(sum|count|avg|max|min)\s*\(', col, re.IGNORECASE)[0].lower()
				aggr_col=re.findall(rf'\((.*)\)', col)[0].strip()
				proj_col.append(aggr_func+'('+aggr_col+')')
			elif re.findall(rf'^{table_column_re}$', col):
				proj_col.append(col)
			else:
				remove_temp_files()
				print_error_message('syntax')
				return
		if aggr_col=='':
			remove_temp_files()
			print_error_message('group1_aggr0')
			return

		cateby_col=cateby_argv[0].strip()
		func_result=group_aggr(input_file, cateby_col, aggr_func, aggr_col)
		if func_result==0:
			return
		input_file='result.csv'

	# sort
	sortby_argv=re.findall(rf'sortby\s+({table_column_re})(\s+descend)?{end_cmd}', cmd, re.IGNORECASE)
	if sortby_argv:
		sortby_col=sortby_argv[0][0].strip()
		ascending=1
		if sortby_argv[0][1]:
			ascending=-1
		func_result=order(input_file, sortby_col, ascending)
		if func_result==0:
			return
		input_file='result.csv'

	# projection
	if proj_col: # proj_col not null means categorize function is activated
		func_result=projection(input_file, proj_col)
		if func_result==0:
			return
		input_file='result.csv'
	else:
		find_argv=re.findall(rf'find\s+(.+)\s+in', cmd, re.IGNORECASE)[0].strip()
		find_col=find_argv.split(',')	
		if len(find_col)==1 and find_col[0].strip()=='*':
			projection(input_file, ['*'])
			input_file='result.csv'
		else:
			for col in find_col:
				col=col.strip()
				if re.findall(rf'(?:sum|count|avg|max|min)\s*\({table_column_re}\)', col, re.IGNORECASE) or \
				   re.findall(r'count\s*\(\s*\*\s*\)', col, re.IGNORECASE):
					remove_temp_files()
					print_error_message('group0_aggr1')
					return
				elif re.findall(rf'^{table_column_re}$', col):
					proj_col.append(col)
				else:
					remove_temp_files()
					print_error_message('syntax')
					return			
			func_result=projection(input_file, proj_col)
			if func_result==0:
				return
			input_file='result.csv'			

	
	

	print_query_result()
