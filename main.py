import parse
from utils import interface, print_error_message

import sys

print(interface+'Welcome to TrojanDB!')

while 1:
	try:
		cmd = input(interface)
	except KeyboardInterrupt: # CTRL+C
		print('\n^C')
		continue
	if cmd.lower() == 'exit': # exit
		print(interface+'Bye!')
		break
	if cmd.strip()[:5].lower() == 'addto':
		parse.insert_cmd(cmd)
	elif cmd.strip()[:5].lower() == 'renew':
		parse.update_cmd(cmd)
	elif cmd.strip()[:6].lower() == 'remove':
		parse.delete_cmd(cmd)
	elif cmd.strip()[:4].lower() == 'find':
		parse.find_cmd(cmd)
	elif len(cmd.strip())==0: # 'enter'
		continue
	elif cmd.strip()[0]=='#': # comment
		continue
	else:
		print_error_message('syntax')
sys.exit(0)