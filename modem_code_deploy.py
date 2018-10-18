#!/usr/bin/python
import requests
import sys
import time
import logging
import pickle
import json

from multiprocessing import Pool

# Setup logging
logging.basicConfig(filename='modem_code_deploy.log',format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

# To be configured with relevant OSS creds
oss_creds = ()
oss_url = ''

def get_all_oss_terminal_packages():
	r = requests.get(
		"https://{}/api/1.0/hts/collections/terminalpackages".format(oss_url), auth=oss_creds)
	data = r.json()
	logging.debug('get_all_oss_terminal_packages: {}'.format(data))
	return r.status_code, data

def get_oss_terminal(terminal_id):
	r = requests.get(
		"https://{}/api/1.0/hts/terminals/?terminal_id={}".format(oss_url,terminal_id), auth=oss_creds)
	data = r.json()
	return r.status_code, data

def get_oss_terminal_config(terminal_obj_id):
	r = requests.get(
		"https://{}/api/1.0/hts/terminals/{}".format(oss_url,terminal_obj_id), auth=oss_creds)
	data = r.json()
	return r.status_code, data

def post_oss_sweeper_templates(template_id, template_and_version, terminal_ids_exclude):
	url = "https://{}/api/1.0/hts/sweepers/terminals/templates".format(oss_url)

	payload = {'template_id': template_id, 'template_and_version': template_and_version,
			'terminal_ids_exclude': terminal_ids_exclude}

	response = requests.post(url, json=payload, auth=oss_creds)
	logging.debug(
		'post_oss_sweeper_templates: Response Code: {}'.format(response.status_code))
	data = response.json()
	logging.info('post_oss_sweeper_templates: {}'.format(data))
	return data

def put_oss_terminal(obj_id, payload):
	url = "https://{}/api/1.0/hts/terminals/{}".format(oss_url,obj_id)
	
	# Example payload
	# payload = { "obj_revision": 2, "enablestaticroutes": true, "static_ip_data_channel_id": 1703 }

	response = requests.put(url, json=payload, auth=oss_creds)
	#data = response.json()
	return response.status_code #, data

def put_oss_terminal_package(obj_id_list, active_sw_obj_id, standby_sw_obj_id):
	url = "https://{}/api/1.0/hts/sweepers/terminals/packages".format(oss_url)
	
	# Example payload
	#payload = '{  "packages": [    {      "active": true,      "package_id": 17220    },    {      "active": false,      "package_id": 18599    }  ],  "terminal_ids": [11100, 19546, 17631, 20615, 17440, 21709, 14782]}'
	
	payload = { "terminal_ids": obj_id_list, "packages": [ { "active": True, "package_id": active_sw_obj_id }, { "active": False, "package_id": standby_sw_obj_id } ] }
	response = requests.put(url, json=payload, auth=oss_creds)
	data = response.json()
	return response.status_code, data

def get_oss_subscriber(subscriber_id):
	r = requests.get(
		"https://{}/api/1.0/hts/subscribers/?subscriber_id={}".format(oss_url,subscriber_id), auth=oss_creds)
	data = r.json()
	logging.debug('get_oss_subscribers: {}'.format(data))
	return r.status_code, data

def get_all_oss_subscriber():
	r = requests.get(
		"https://{}/api/1.0/hts/subscribers/".format(oss_url), auth=oss_creds)
	data = r.json()
	logging.info('get_oss_subscribers: {}'.format(r))
	return r.status_code, data

def get_all_oss_terminals():
	r = requests.get(
		"https://{}/api/1.0/hts/terminals/".format(oss_url), auth=oss_creds)
	data = r.json()
	logging.debug('get_oss_terminalss: {}'.format(data))
	return r.status_code, data

def get_oss_subscriber_obj_id(obj_id):
	r = requests.get(
		"https://{}/api/1.0/hts/subscribers/{}".format(oss_url,obj_id), auth=oss_creds)
	data = r.json()
	#logging.debug('get_oss_subscribers: {}'.format(data))
	return r.status_code, data

def patch_oss_subscriber(obj_id, plan):
	payload = {'subscriber_plan_id': plan, 'subsriber_detailed_report': 'true'}
	r = requests.patch(
		"https://{}/api/1.0/hts/subscribers/{}".format(oss_url,obj_id), json=payload, auth=oss_creds)
	return r.status_code

def get_oss_async_status(obj_id):
	r = requests.get(
		"https://{}/api/1.0/hts/terminals/async/status?obj_id={}".format(oss_url,obj_id), auth=oss_creds)
	data = r.json()
	return r.status_code, data

def post_oss_terminal_EDk(edk, template_id, terminal_ids):
	url = "https://{}/api/1.0/hts/sweepers/terminals/customkeys".format(oss_url)

	payload = {'keys':  {"BEAM_SELECTOR": {"asc_mode": 1}}, 'template_id': template_id,
			'terminal_ids': terminal_ids}
	# {
	#   "keys": {"BEAM_SELECTOR": {"asc_mode": 1}},
	#   "template_id": "CONUS_STANDARD",
	#   "terminal_ids": [
	#     338378
	#   ]
	# }
	response = requests.post(url, json=payload, auth=oss_creds)
	logging.info('post_oss_terminal_EDk: Response Code: {}'.format(response.status_code))
	data = response.json()
	logging.debug('post_oss_terminal_EDk: {}'.format(data))
	return response.status_code, data
	
def get_oss_terminals_status(terminal_obj_id):
	r = requests.get(
		"https://{}/api/1.0/hts/terminals/{}/status".format(oss_url,terminal_obj_id), auth=oss_creds)
		
	data = r.json()
	#logging.debug('get_oss_subscribers: {}'.format(data))
	return r.status_code, data

def write_active_terminals_to_file(active_terminals):
	with open('active_terminals_store.pkl', 'wb') as output:
		pickle.dump(active_terminals, output)

def read_active_terminals_from_file():
	with open('active_terminals_store.pkl', 'rb') as input:
		return pickle.load(input)

def fix_qos(terminal):
	def apply_plan(subscriber):
		respCode = patch_oss_subscriber(subscriber['obj_id'], subscriber['subscriber_plan_id'])
		if respCode == 204:
			logging.info('{} plan apply success'.format(subscriber['subscriber_id']))
		else:
			logging.warning('{} plan apply failure function returned {}'.format(subscriber['subscriber_id'],respCode))
	hs_sub_resp_code, hs_sub = get_oss_subscriber(terminal+'-01')
	ul_sub_resp_code, ul_sub = get_oss_subscriber(terminal+'-02')

	if hs_sub_resp_code == 200:
		apply_plan(hs_sub)
	else:
		logging.warning('fix qos subscriber lookup failed {} {} {}'.format(terminal,ul_sub_resp_code, ul_sub))
	if ul_sub_resp_code == 200:
		apply_plan(ul_sub)
	else:
		logging.warning('fix qos subscriber lookup failed {} {} {}'.format(terminal,ul_sub_resp_code, ul_sub))

# TODO Fix statics function does not handle template region changes
def fix_statics(terminal_id, static_payload):
	# Look up the obj_id
	status, terminal = get_oss_terminal(terminal_id)
	if status == 200:
		# new_terminal_config_status, new_terminal_config = get_oss_terminal_config(terminal[0]['obj_id'])
		# if new_terminal_config_status == 200:
		# 	new_data_channels = new_terminal_config['data_channels']
			# Loop through static payloads
		for payload in static_payload:
			# Post payload to fix satic
			status_code = put_oss_terminal(terminal[0]['obj_id'], payload)
			if status_code == 204:
				logging.info('{} static fixed for {}'.format(payload['static_ip_data_channel_id'],terminal_id))
			else:
				logging.warning('Error trying to fix {} static for {} {}'.format(payload['static_ip_data_channel_id'],terminal_id, payload))
	else:
		logging.warning('Error trying to fix static, unable to look up obj_id for {} {}'.format(terminal_id, terminal))

def monitor_async(job_obj):
	isPending = True
	while isPending:
		status_code, status = get_oss_async_status(job_obj['obj_id'])
		try:
			isPending = not status['complete']
			if status['complete'] == True:
				if status['result'] == True:
					return (True, job_obj)
				if status['result'] == False:
					#logging.warning('{} result: {} message: {}'.format(job_obj['terminal']['terminal_id'], status['result'], status['message']))
					logging.warning('Payload for failed terminal: {}'.format(job_obj))
					return (False, job_obj)
		except KeyError:
			isPending = False
		time.sleep(10)
	return (False, job_obj)

def make_terminal_config_dict(some_obj_ids, destination_template):
	conus_dc = [1703,1704]
	emea_dc = [1705,1706]
	asia_dc = [1707,1708]

	if 'CONUS_STANDARD' in destination_template:
		dest_dc = conus_dc
	elif 'EMEA_STANDARD' in destination_template:
		dest_dc = emea_dc
	elif 'ASIA_STANDARD' in destination_template:
		dest_dc = asia_dc

	terminal_static_dict = {}

	for obj_id in some_obj_ids:
		# Pull terminals config based on obj_id
		term_config_response_code, term_config = get_oss_terminal_config(obj_id)

		if term_config_response_code == 200:
			# Var to hold payloads for terminals static fixes
			static_payload = []

			if term_config['data_channels'][0]['enablestaticroutes'] == True:
				# Create json payload to post in after terminal is rebuit
				static_payload.append({ "obj_revision": term_config['obj_revision'], "enablestaticroutes": True, "static_ip_data_channel_id": dest_dc[0] })
			
			if term_config['data_channels'][1]['enablestaticroutes'] == True:
				# Create json payload to post in after terminal is rebuit
				static_payload.append({ "obj_revision": term_config['obj_revision'], "enablestaticroutes": True, "static_ip_data_channel_id": dest_dc[1] })
			
			# Check if the list has some length
			if len(static_payload) > 0:
				terminal_static_dict[term_config['terminal_id']] = static_payload
		else:
			logging.warning('Failed to lookup terminal config for {} {} {}'.format(obj_id,term_config_response_code, term_config))
	#return dict
	#logging.info(terminal_static_dict)
	return terminal_static_dict

def shall_we_proceed():
	uinput = raw_input("Press 'c' to continue or 'a' to abort...")
	if uinput == 'c':
		logging.info('Continue...')
		return True
	else:
		logging.info('ABORT')
		return False

def create_exclusion(someTemplateToRebuild, somePlanToRebuild, someNumberofTerminals):
	term_scode, terminals = get_all_oss_terminals()
	sub_scode, subscribers = get_all_oss_subscriber()

	subscriber_dict = {}
	obj_id_list = []

	number_of_terminals_about_to_be_rebuilt = 0

	if term_scode == 200 and sub_scode == 200:
		for subscriber in subscribers:
			subscriber_dict[int(subscriber['subscriber_id'][:-3])] = subscriber['subscriber_plan_id']

		for terminal in terminals:
			try:
				if terminal['contactnote'] == someTemplateToRebuild:
					if number_of_terminals_about_to_be_rebuilt < someNumberofTerminals:
						logging.info('{} on plan {} to be rebuilt'.format(terminal['terminal_id'],somePlanToRebuild))
						number_of_terminals_about_to_be_rebuilt += 1

					else:
						obj_id_list.append(terminal['obj_id'])
						logging.info('{} added to exclusion list'.format(terminal['terminal_id']))

			except KeyError:
				logging.warning('Terminal ID {} failed subscriber lookup'.format(terminal['terminal_id']))
		logging.info('{} terminals in the exclusion list'.format(str(len(obj_id_list))))

	else:
		logging.warning('create exclution get_all_oss_terminals() returned {}'.format(terminals))

	logging.warning('{} terminals to be rebuilt'.format(number_of_terminals_about_to_be_rebuilt))
	return obj_id_list

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in xrange(0, len(l), n))

if __name__ == "__main__":
	logging.info('START')

	# Load in config
	config = json.load(open('config.json'))
	# Init rebuild dict
	deploy_parameters = config['modem_code_deploy_parameters']

	# Determine environment
	if deploy_parameters['environment'] == 'prod':
		oss_creds = (config['prod_environment']['prod_usr'], config['prod_environment']['prod_pw'])
		oss_url = config['prod_environment']['prod_url']
	elif deploy_parameters['environment'] == 'dev':
		oss_creds = (config['dev_environment']['dev_usr'], config['dev_environment']['dev_pw'])
		oss_url = config['dev_environment']['dev_url']
	else:
		logging.warning('Invaild environment.. Try "prod" or "dev" ')
		sys.exit(0)

	# log rebuild parameters
	logging.info('Modem Code Deploy Parameters')
	logging.info('Environment: {}'.format(deploy_parameters['environment']))
	logging.info('Continue previous deploy: {}'.format(deploy_parameters['continue_previous_deploy']))
	logging.info('Population: {}'.format(deploy_parameters['population']))
	logging.info('Active software: {}'.format(deploy_parameters['active_software']))
	logging.info('Passive software: {}'.format(deploy_parameters['standby_software']))
	logging.info('Exception list: {}'.format(deploy_parameters['exception_list']))
	logging.info('Chunk size: {}'.format(deploy_parameters['chunk_size']))
	logging.info('Continuous: {}'.format(deploy_parameters['continuous']))

	# Store obj_id's for terminals to be modified
	active_terminals = []

	# Store Terminal ID's for terminals to be modified 
	human_readable_active_terminals = []

	# Pick up where last deploy left off with current config settings
	if deploy_parameters["continue_previous_deploy"] == True:
		try:
			# Load file containing previous runs remaining terminals
			active_terminals = read_active_terminals_from_file()
			logging.info('{} active terminals loaded from file!'.format(len(active_terminals)))
		except:
			# File may not exist
			logging.critical("failed to load active terminal file")
	
	else:
		# get all subscribers
		sub_scode, subscribers = get_all_oss_subscriber()

		if sub_scode == 200:
			
			for sub in subscribers:
				
				# Active: terminals that have paying subsribers behind them A.K.A a terminal with a subscription other than enabled
				if deploy_parameters['population'] == 'active':
					
					# 1.) Look at only -01 (avoids adding terminal once)
					# 2.) Make sure terminal has a paying subscription
					# 3.) Make sure terminal is not in the exception list
					if ('-01' in sub['subscriber_id'] and
							'Enabled' not in sub['subscriber_plan_id'] and
							sub['subscriber_id'].replace('-01','') not in deploy_parameters['exception_list']):

						# Look up terminal obj_id
						term_scode, terminal_obj = get_oss_terminal(sub['subscriber_id'].replace('-01',''))
						
						# Make sure lookup was a success and returned at least on obj
						if term_scode == 200 and len(terminal_obj) == 1:
							logging.info("{} to get new modem code".format(sub['subscriber_id'].replace('-01','')))

							# Add terminal obj id to the list of active terminals
							active_terminals.append(terminal_obj[0]['obj_id'])

							# Add terminal ID to a list of human readable terminals
							human_readable_active_terminals.append(sub['subscriber_id'].replace('-01',''))

						else:
							logging.warning('Failed terminal lookup for: {}'.format(sub['subscriber_id']))
				
				# Dormant: terminals that do not have paying subscribers behind them A.K.A a terminal with a subscription of enabled
				elif deploy_parameters['population'] == 'dormant':

					# 1.) Look at only -01 (avoids adding terminal once)
					# 2.) Make sure terminal is in enabled plan
					# 3.) Make sure terminal is not in the exception list
					if ('-01' in sub['subscriber_id'] and
							'Enabled' in sub['subscriber_plan_id'] and
							sub['subscriber_id'].replace('-01','') not in deploy_parameters['exception_list']):

						# Look up terminal obj_id
						term_scode, terminal_obj = get_oss_terminal(sub['subscriber_id'].replace('-01',''))
						
						# Make sure lookup was a success and returned at least on obj
						if term_scode == 200 and len(terminal_obj) == 1:
							logging.info("{} to get new modem code".format(sub['subscriber_id'].replace('-01','')))

							# Add terminal obj id to the list of active terminals
							active_terminals.append(terminal_obj[0]['obj_id'])

							# Add terminal ID to a list of human readable terminals
							human_readable_active_terminals.append(sub['subscriber_id'].replace('-01',''))

						else:
							logging.warning('Failed terminal lookup for: {}'.format(sub['subscriber_id']))


			logging.info("Number of terminals to rebuild: {}".format(len(active_terminals)))
			logging.info("Terminal ID's: {}".format(human_readable_active_terminals))
		else:
			logging.warning('Failed to get all subscribers')
		# write active terminals to file
		write_active_terminals_to_file(active_terminals)

	# Store obj_ids of active sw
	active_sw_obj_id = 0
	# Store obj_ids of standby sw
	standby_sw_obj_id = 0

	# Read in all software packages and map name of package to obj_id
	sw_status_code, sw_data = get_all_oss_terminal_packages()
	if sw_status_code == 200:
		for sw_package in sw_data:
			if sw_package['obj_name'] == deploy_parameters['active_software']:
				active_sw_obj_id = sw_package['obj_id']
			elif sw_package['obj_name'] == deploy_parameters['standby_software']:
				standby_sw_obj_id = sw_package['obj_id']
	else:
		logging.warning("Software package call failed. {}".format(sw_data))
		sys.exit(0)

	# if lookups fail exit program
	if not active_sw_obj_id or not standby_sw_obj_id:
		logging.warning('One of the software packages strings did not match a string in terminal packages response')
		sys.exit(0)
		
	# Prompt user for imput
	if shall_we_proceed():
		# Iterate overall active terminals
		chunk_size = deploy_parameters["chunk_size"]
		terminal_chunks = chunks(active_terminals, chunk_size)
		chunk_number = 0
		for chunk in terminal_chunks:
			logging.info("Processing chunk number: {}".format(chunk_number))
			logging.debug("Processing terminal obj_ids: {}".format(chunk))
			status, job_ids = put_oss_terminal_package(chunk, active_sw_obj_id, standby_sw_obj_id)
			#logging.info("Current jobs: {}".format(job_ids))
			if status == 202:

				# Monitor Template Rebuild Jobs
				is_async_task_pending = True
				pending_async_tasks = len(job_ids)
				while is_async_task_pending:
					is_async_task_pending = False
					
					time.sleep(config['general']['async_task_status_check_interval'])

					for job_id in job_ids:
						status_code, status = get_oss_async_status(job_id['obj_id'])
						if status_code == 200:
							if status['complete'] == False:
								is_async_task_pending = True
							else:
								if status['result'] == True:
									logging.info(status)
									pending_async_tasks -=1
								else:
									logging.warning(status)
									pending_async_tasks -=1

					#logging.info('{} pending rebuilds'.format(pending_async_tasks))

				for term in chunk:
					t_scode, t_status = get_oss_terminals_status(term)

					if t_scode == 200:
						logging.info(t_status['obj_name'])
					else:
						logging.warning(term, t_scode, t_status)

	
				#chunk complete
				chunk_number += 1
				write_active_terminals_to_file(active_terminals[chunk_size*chunk_number:])
				logging.info('{} terminals remaining in modem code deploy'.format(len(active_terminals[chunk_size*chunk_number:])))
				logging.info('Remaining active terminals written to disk')

			if deploy_parameters['continuous'] == False:
				
				# wait for user input
				if shall_we_proceed():
					pass
				else:
					sys.exit(0)

			else:
				time.sleep(deploy_parameters['chunk_interval'])

	logging.info('END')
