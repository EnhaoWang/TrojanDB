import sys
import os
import csv
import re

from utils import interface, print_error_message, chunksize, remove_temp_files

def value_comparison(value1, value2, operator):
	operators_mapping = {
	        '=': lambda x, y: x == y,
	        '>': lambda x, y: x > y,
	        '<': lambda x, y: x < y,
	        '>=': lambda x, y: x >= y,
	        '<=': lambda x, y: x <= y,
	        '!=': lambda x, y: x != y}
	if value1=='NULL' or value2=='NULL':
		if operator not in ['=', '!=']:
			return False
	try: return operators_mapping[operator](float(value1), float(value2))
	except: return operators_mapping[operator](value1, value2)

def insert(input_file, new_values):
	temp_file = 'temp.csv'

	with open(input_file, 'r', newline='') as input_csv, open(temp_file, 'w', newline='') as output_csv:

		csv_reader = csv.reader(input_csv)
		csv_writer = csv.writer(output_csv)
		
		headers=next(csv_reader)
		csv_writer.writerow(headers)

		# check whether columns exists in the table
		row_data=['NULL' for i in range(len(headers))]

		for column, value in new_values:
			if column not in headers:
				remove_temp_files()
				print_error_message('missing_column', column)
				return 0
			row_data[headers.index(column)]=value

		while 1:
			chunk=[]
			end=0
			for i in range(chunksize):
				try:
					chunk.append(next(csv_reader))
				except:
					end=1
					break
			if not chunk:
				csv_writer.writerow(row_data)
				break
			if end==1:
				chunk.append(row_data)
				csv_writer.writerows(chunk)
				break
			else:
				csv_writer.writerows(chunk)

	input_csv.close()
	output_csv.close()

	# delete the original file and rename the temp file
	os.remove(input_file)
	os.rename(temp_file, input_file)
	print(interface+'New data is inserted!')

def update(input_file, new_values, conditions=[], logic='and'):
	temp_file = 'temp.csv'

	with open(input_file, 'r', newline='') as input_csv, open(temp_file, 'w', newline='') as output_csv:

		csv_reader = csv.reader(input_csv)
		csv_writer = csv.writer(output_csv)
		
		headers=next(csv_reader)
		csv_writer.writerow(headers)

		# check whether columns exists in the table
		for column, value in new_values:
			if column not in headers:
				remove_temp_files()
				print_error_message('missing_column', column)
				return 0
		for column, operator, value in conditions:
			if column not in headers:
				remove_temp_files()
				print_error_message('missing_column', column)
				return 0
			if value=='NULL' and operator not in ['=', '!=']:
				remove_temp_files()
				print_error_message('null')
				return 0
 
		while 1:
			chunk=[]
			end=0
			for i in range(chunksize):
				try:
					chunk.append(next(csv_reader))
				except:
					end=1
					break
			if not chunk:
				break
			# for each line in the file, check if it meets the conditons, then write a line on the temporary file
			written_data=[]
			for row in chunk:	
				if logic=='or': # conditions are connected by 'or'
					satisfy=0
					for column, operator, value2 in conditions:
						value1=row[headers.index(column)]
						if value_comparison(value1,value2,operator):
							satisfy=1
							break
				else:
					satisfy=1
					for column, operator, value2 in conditions:
						value1=row[headers.index(column)]
						if not value_comparison(value1,value2,operator):
							satisfy=0
							break

				if satisfy==1: # update the row
					for new_value in new_values:
						row[headers.index(new_value[0])]=new_value[1]
				written_data.append(row)
			
			csv_writer.writerows(written_data)

			if end==1:
				break

	input_csv.close()
	output_csv.close()

	# delete the original file and rename the temp file
	os.remove(input_file)
	os.rename(temp_file, input_file)

	print(interface+'Data is updated!')

def delete(input_file, conditions=[], logic='and'):
	temp_file = 'temp.csv'

	with open(input_file, 'r', newline='') as input_csv, open(temp_file, 'w', newline='') as output_csv:

		csv_reader = csv.reader(input_csv)
		csv_writer = csv.writer(output_csv)
		
		headers=next(csv_reader)
		csv_writer.writerow(headers)

		# check whether columns exists in the table
		for column, operator, value in conditions:
			if column not in headers:
				remove_temp_files()
				print_error_message('missing_column', column)
				return 0
			if value=='NULL' and operator not in ['=', '!=']:
				remove_temp_files()
				print_error_message('null')
				return 0

		while 1:
			chunk=[]
			end=0
			for i in range(chunksize):
				try:
					chunk.append(next(csv_reader))
				except:
					end=1
					break
			if not chunk:
				break
			# for each line in the file, check if it meets the conditons, then write a line on the temporary file
			written_data=[]
			for row in chunk:	
				if logic=='or': # conditions are connected by 'or'
					satisfy=0
					for column, operator, value2 in conditions:
						value1=row[headers.index(column)]
						if value_comparison(value1,value2,operator):
							satisfy=1
							break
				else:
					satisfy=1
					for column, operator, value2 in conditions:
						value1=row[headers.index(column)]
						if not value_comparison(value1,value2,operator):
							satisfy=0
							break
				if satisfy==0:
					written_data.append(row)
					
			csv_writer.writerows(written_data)

			if end==1:
				break

	input_csv.close()
	output_csv.close()

	# delete the original file and rename the temp file
	os.remove(input_file)
	os.rename(temp_file, input_file)

	print(interface+'Data is deleted!')

# the result of following functions are stored in result.csv
def join(file1, file2):
	small_chunksize=2
	temp_file='temp.csv'
	with open(file1, 'r', newline='') as input_csv1, open(file2, 'r', newline='') as input_csv2, open(temp_file, 'w', newline='') as output_csv:
		csv_reader1, csv_reader2=csv.reader(input_csv1), csv.reader(input_csv2)
		csv_writer=csv.writer(output_csv)
		header1, header2=next(csv_reader1), next(csv_reader2)
		table1, table2=file1[10:-4], file2[10:-4]
		for i in range(len(header1)):
			if '.' not in header1[i]:
				header1[i]=table1+'.'+header1[i]
		for i in range(len(header2)):
			header2[i]=table2+'.'+header2[i]
		csv_writer.writerow(header1+header2)

		while 1:
			chunk1=[]
			end1=0
			for i in range(small_chunksize):
				try:
					chunk1.append(next(csv_reader1))
				except:
					end1=1
					break
			if not chunk1:
				break
			while 1:
				chunk2=[]
				end2=0
				for j in range(small_chunksize):
					try:
						chunk2.append(next(csv_reader2))
					except:
						end2=1
						input_csv2.seek(0)
						headers=next(csv_reader2)
						break
				if not chunk2:
					break
				for row1 in chunk1:
					for row2 in chunk2:
						csv_writer.writerow(row1+row2)
				if end2==1:
					break
			if end1==1:
				break

	input_csv1.close()
	input_csv2.close()
	output_csv.close()

	# delete 'result.csv' and rename the temp file
	result_csv='result.csv'
	try: os.remove(result_csv)
	except: pass
	os.rename(temp_file, result_csv)

def filter(input_file, conditions, logic):
	temp_file = 'temp.csv'
	with open(input_file, 'r', newline='') as input_csv, open(temp_file, 'w', newline='') as output_csv:

		csv_reader = csv.reader(input_csv)
		csv_writer = csv.writer(output_csv)
		
		headers=next(csv_reader)
		csv_writer.writerow(headers)

		# check whether columns exists in the table
		for column, operator, value in conditions:
			if column not in headers:
				remove_temp_files()
				print_error_message('missing_column', column)
				return 0
			if value=='NULL' and operator not in ['=', '!=']:
				remove_temp_files()
				print_error_message('null')
				return 0

		while 1:
			chunk=[]
			end=0
			for i in range(chunksize):
				try:
					chunk.append(next(csv_reader))
				except:
					end=1
					break
			if not chunk:
				break
			# for each line in the file, check if it meets the conditons, then write a line on the temporary file
			written_data=[]
			for row in chunk:	
				if logic=='or': # conditions are connected by 'or'
					satisfy=0
					for column, operator, value2 in conditions:
						value1=row[headers.index(column)]
						if value2 in headers:
							value2=row[headers.index(value2)] 
						if value_comparison(value1,value2,operator):
							satisfy=1
							break
				else:
					satisfy=1
					for column, operator, value2 in conditions:
						value1=row[headers.index(column)]
						if value2 in headers:
							value2=row[headers.index(value2)]
						if not value_comparison(value1,value2,operator):
							satisfy=0
							break
				if satisfy==1:
					written_data.append(row)

			csv_writer.writerows(written_data)

			if end==1:
				break

	input_csv.close()
	output_csv.close()

	# delete 'result.csv' and rename the temp file
	result_csv='result.csv'
	try: os.remove(result_csv)
	except: pass
	os.rename(temp_file, result_csv)


def group_aggr(input_file, cateby_col, aggr_func, aggr_col):
    aggr_info = 'aggr.csv'
    temp_file = 'temp.csv'
    info_chunksize=10
    with open(aggr_info, 'w+', newline=''):
        pass
    with open(input_file, 'r', newline='') as input_csv:
        reader = csv.reader(input_csv)
        
        header = next(reader)
        if cateby_col not in header: 
            remove_temp_files()
            print_error_message('missing_column', cateby_col)
            return 0
        if aggr_col not in header and aggr_col!='*':
            remove_temp_files()
            print_error_message('missing_column', aggr_col)
            return 0

        by_indx=header.index(cateby_col)
        if aggr_col!='*':
            aggr_indx=header.index(aggr_col)

        # fill in the information of groups
        while 1:
            aggr_dict={}
            data_chunk=[]
            end1=0
            for i in range(int(chunksize/2)):
                try:
                    data_chunk.append(next(reader))
                except:
                    end1=1
                    break

            if not data_chunk:
                break
            for row in data_chunk:
                if row[by_indx]=='NULL':
                    continue
                if row[by_indx] not in aggr_dict:
                    aggr_dict[row[by_indx]]={'max':'NULL', 'min':'NULL', 'sum':'NULL', 'count_all':0, 'count_notnull':0, 'count_num':0}
                if aggr_col!='*':
                    value=row[aggr_indx]
                    if value=='NULL':
                        aggr_dict[row[by_indx]]['count_all']+=1
                    else:
                        try: 
                            value=float(value)
                            if aggr_dict[row[by_indx]]['max']=='NULL' or value>aggr_dict[row[by_indx]]['max']:
                                aggr_dict[row[by_indx]]['max']=value
                            if aggr_dict[row[by_indx]]['min']=='NULL' or value<aggr_dict[row[by_indx]]['min']:
                                aggr_dict[row[by_indx]]['min']=value
                            if aggr_dict[row[by_indx]]['sum']=='NULL':
                                aggr_dict[row[by_indx]]['sum']=0
                            aggr_dict[row[by_indx]]['sum']+=value
                            aggr_dict[row[by_indx]]['count_all']+=1
                            aggr_dict[row[by_indx]]['count_notnull']+=1
                            aggr_dict[row[by_indx]]['count_num']+=1
                        except: 
                            aggr_dict[row[by_indx]]['count_all']+=1
                            aggr_dict[row[by_indx]]['count_notnull']+=1
                else:
                    aggr_dict[row[by_indx]]['count_all']+=1

            # check for groups that don't have numbers to modify the aggregation function results:
            # for col_name in aggr_dict.keys():
            #     if aggr_dict[col_name]['count_num']==0:
            #         aggr_dict[col_name]['max']='NULL'
            #         aggr_dict[col_name]['min']='NULL'

            # if not os.path.exists(aggr_info):
                # with open(aggr_info, 'w+', newline=''):
                #     pass
            
            with open(aggr_info, 'r+', newline='') as aggr_file, open(temp_file, 'w', newline='') as output_csv:

                csv_reader=csv.reader(aggr_file)

                csv_writer=csv.writer(output_csv)
                while 1:
                    info_chunk=[]
                    end2=0
                    for j in range(info_chunksize):
                        try:
                            info_chunk.append(next(csv_reader))
                        except:
                            end2=1
                            break

                    if not info_chunk:
                        break
                    for group in info_chunk:
                        
                        if group[0] in aggr_dict.keys():
                            group_info=aggr_dict[group[0]]

                            if group[1]=='NULL':
                                group[1]=group_info['max']
                                group[2]=group_info['min']
                                group[3]=group_info['sum']
                            else:
                                if group_info['max']!='NULL':                                
                                    group[1]=max(float(group[1]), float(group_info['max']))
                                    group[2]=min(float(group[2]), float(group_info['min']))
                                    group[3]=float(group[3])+float(group_info['sum'])
               
                            group[4]=int(group[4])+int(group_info['count_all'])
                            group[5]=int(group[5])+int(group_info['count_notnull'])
                            group[6]=int(group[6])+int(group_info['count_num'])

                            del aggr_dict[group[0]]

                        csv_writer.writerow(group)

                    if end2==1:
                        break

                for key, value in aggr_dict.items():
                    csv_writer.writerow([key, value['max'], value['min'], value['sum'], value['count_all'], value['count_notnull'], value['count_num']])
            
            os.remove(aggr_info)
            os.rename(temp_file, aggr_info)

            if end1==1:
                break

    with open(aggr_info, 'r', newline='') as input_csv, open('result.csv', 'w', newline='') as output_csv:
        csv_reader = csv.reader(input_csv)
        csv_writer = csv.writer(output_csv)
        csv_writer.writerow([cateby_col, aggr_func+'('+aggr_col+')'])
        while 1:
            info_chunk=[]
            end=0
            for i in range(info_chunksize):
                try:
                    info_chunk.append(next(csv_reader))
                except:
                    end1=1
                    break
            if not info_chunk:
                break

            for group in info_chunk:
                if aggr_func=='count':
                    if aggr_col=='*':
                        csv_writer.writerow([group[0], group[4]])
                    else:
                        csv_writer.writerow([group[0], group[5]])
                elif aggr_func=='avg':
                    avg='NULL' if group[3]=='NULL' else float(group[3])/float(group[6])
                    csv_writer.writerow([group[0], avg])
                else:
                    mapping={'max':1, 'min': 2, 'sum':3}
                    csv_writer.writerow([group[0], group[mapping[aggr_func]]])

    os.remove(aggr_info)


def projection(input_file, columns_to_keep):
    temp_file = 'temp.csv'
    with open(input_file, 'r', newline='') as input_csv, open(temp_file, 'w', newline='') as output_csv:
        csv_reader = csv.reader(input_csv)
        csv_writer = csv.writer(output_csv)
        
        header = next(csv_reader)
        if columns_to_keep==['*']:
        	columns_to_keep=header

        for column in columns_to_keep:
            if column not in header: 
            	remove_temp_files()
            	print_error_message('missing_column', column)
            	return 0
        header_indices = [header.index(column) for column in columns_to_keep]
        csv_writer.writerow(columns_to_keep)

        while 1:
            chunk=[]
            end=0
            for i in range(chunksize):
                try:
                    chunk.append(next(csv_reader))
                except:
                    end=1
                    break
            if not chunk:
                break
            for i in range(len(chunk)):
                chunk[i] = [chunk[i][j] for j in header_indices]
            csv_writer.writerows(chunk)

    input_csv.close()
    output_csv.close()

    # delete 'result.csv' and rename the temp file
    result_csv='result.csv'
    try: os.remove(result_csv)
    except: pass
    os.rename(temp_file, result_csv)


def order(input_file, sortby_col, ascending=1):
	def custom_sort(chunk, n, ascending=True):
		def is_number(s):
			try:
				float_value = float(s)
				return True
			except ValueError:
				return False

		def compare_elements(elem1, elem2, ascending):
			# 定义比较函数，根据规则比较两个元素
			# if elem1>elem2, return positive
			if is_number(elem1[n]) and is_number(elem2[n]):
				# 如果两个元素都是数字
				return 1 if float(elem1[n]) > float(elem2[n]) else -1
			elif is_number(elem1[n]):
				# elem1是数字，elem2是字符串
				return (-1)*ascending
			elif is_number(elem2[n]):
				# elem1是字符串，elem2是数字
				return 1*ascending
			elif elem1[n] == 'NULL' and elem2[n] == 'NULL':
				# 如果两个元素都是'NULL'
				return 0
			elif elem1[n] == 'NULL':
				# elem1是'NULL'，elem2不是'NULL'
				return 1*ascending
			elif elem2[n] == 'NULL':
				# elem1不是'NULL'，elem2是'NULL'
				return (-1)*ascending
			else:
				# 两个元素都是字符串
				return (elem1[n] > elem2[n]) - (elem1[n] < elem2[n])

		# 冒泡排序
		for i in range(len(chunk)):
			for j in range(0, len(chunk)-i-1):
				compare_result=compare_elements(chunk[j], chunk[j+1], ascending)
				if (ascending==1 and compare_result > 0) or (ascending==-1 and compare_result < 0):
					chunk[j], chunk[j+1] = chunk[j+1], chunk[j]

	def find_min_or_max_index(rows, n, ascending):
		def try_float(s):
			try:
				return float(s)
			except ValueError:
				return None
			
		values=[row[n] for row in rows]

		# find min float
		float_indices = [i for i, s in enumerate(values) if try_float(s) is not None]
		if float_indices:
			if ascending==1:
				return min(float_indices, key=lambda i: try_float(values[i]))
			else:
				return max(float_indices, key=lambda i: try_float(values[i]))

		# no float, find min str
		non_null_indices = [i for i, s in enumerate(values) if s != 'NULL']
		if non_null_indices:
			if ascending==1:
				return min(non_null_indices, key=lambda i: values[i])
			else:
				return max(non_null_indices, key=lambda i: values[i])

		# all are 'NULL'
		return 0

	with open(input_file, 'r', newline='') as input_csv:
		csv_reader=csv.reader(input_csv)
		headers=next(csv_reader)

		# check whether columns exists in the table
		if sortby_col not in headers:
			remove_temp_files()
			print_error_message('missing_column', sortby_col)
			return 0
		sortby_index=headers.index(sortby_col)

		chunk_index=1
		while 1:
			chunk=[]
			end=0
			for i in range(chunksize):
				try:
					chunk.append(next(csv_reader))
				except:
					end=1
					break
			if not chunk:
				break

			custom_sort(chunk,sortby_index, ascending)
			
			with open(f'chunk{chunk_index}.csv', 'w', newline='') as chunk_csv:
				csv_writer=csv.writer(chunk_csv)
				csv_writer.writerows(chunk)
			chunk_index+=1

			if end==1:
				break

	chunk_index=1
	csv_readers=[]
	while 1:
		try:
			csv_readers.append(csv.reader(open(f'chunk{chunk_index}.csv', 'r', newline='')))
			chunk_index+=1
		except:
			break

	temp_file = 'temp.csv'
	with open(temp_file, 'w', newline='') as output_csv:
		csv_writer=csv.writer(output_csv)
		csv_writer.writerow(headers)

		rows=[next(reader) for reader in csv_readers]
		while 1:
			if not rows:
				break
			index=find_min_or_max_index(rows,sortby_index,ascending)
			csv_writer.writerow(rows[index])
			try:
				rows[index]=next(csv_readers[index])
			except:
				del rows[index]
				del csv_readers[index]
			
	
	# delete chunk files
	chunk_index=1
	while 1:
		try:
			os.remove(f'chunk{chunk_index}.csv')
			chunk_index+=1
		except: break

	# delete 'result.csv' and rename the temp file
	result_csv='result.csv'
	try: os.remove(result_csv)
	except: pass
	os.rename(temp_file, result_csv)
	

def print_query_result():
	print(interface+'Query result:')
	with open('result.csv', 'r', newline='', encoding='utf-8') as file:

		csv_reader = csv.reader(file)
		header = next(csv_reader)
		max_lengths=[len(column) for column in header]
		num=len(header)

		while 1:
			chunk=[]
			end=0
			for i in range(chunksize):
				try:
					chunk.append(next(csv_reader))
				except:
					end=1
					break
			if not chunk:
				break
			for row in chunk:
				for i in range(num):
					try:
						float_num=float(row[i])
						if float_num.is_integer():
							int_num=int(row[i])
							max_lengths[i]=max(max_lengths[i], len(str(int_num)))
						else:
							# float_num=round(float_num, 3)
							max_lengths[i]=max(max_lengths[i], len(str(float_num)))
					except:
						max_lengths[i]=max(max_lengths[i], len(row[i]))
			if end==1:
				break

		header_str = " | ".join(header[i].ljust(max_lengths[i]) for i in range(num))
		print(f"{'-' * len(header_str)}")
		print(header_str)
		print(f"{'-' * len(header_str)}")

		file.seek(0)
		header=next(csv_reader)

		while 1:
			chunk=[]
			end=0
			for i in range(chunksize):
				try:
					chunk.append(next(csv_reader))
				except:
					end=1
					break
			if not chunk:
				break

			# read data and print
			for row in chunk:
				for i in range(num):
					try:
						float_num=float(row[i])
						if float_num.is_integer():
							row[i]=str(int(float_num))
						# else:
							# row[i]=str(round(float_num, 3))
					except:
						pass
				row_str = " | ".join(row[i].ljust(max_lengths[i]) for i in range(num))
				print(row_str)

			if end==1:
				break

		print(f"{'-' * len(header_str)}")

	remove_temp_files()







