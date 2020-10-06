from scrapy import Spider
from scrapy.http import Request
from scrapy.selector import Selector

from seleniumwire import webdriver
from requests import get

from json import loads


import os, logging
from datetime import date

from time import sleep
from socket import gethostbyname, create_connection, error



def check_connection():
	while True:
		try:
			gethostbyname('google.com')
			connection = create_connection(('google.com', 80), 1)
			connection.close()
			print('Hay conexion a internet, continuamos !!')
			break
		
		except error:
			print('No hay conexion a internet, esperaremos por 2 minutos')
			sleep(3)
			continue


check_connection()


N_category = -1



def print_(message = ' ', type_ = 'info'):

	message = str(message)
	if type_ == 'deb': # deb es igual a debug
		logging.debug(message)
		print(message)
	elif type_ == 'info': # info es igual a informacion
		logging.info(message)
		print(message)
	elif type_ == 'war': # war es igual a warning
		logging.warning(message)
		print(message)
	else:
		print('Mire que es corto lo que hay que escribir pa que se equivoque!!')

class FincaraizScrapingCasas2Spider(Spider):
	name = 'fincaraiz_scraping_casas_2'
	allowed_domains = ['fincaraiz.com.co']
	start_urls = ['https://www.fincaraiz.com.co/']

	def parse(self, response):

		try:
			LOG_FILENAME = '.\\logs\\casas_2_log_' + str(date.today()) + '.log'

			for handler in logging.root.handlers[:]:
				logging.root.removeHandler(handler)
			
			logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG, format= '%(asctime)s : %(levelname)s : %(message)s')    
			logging.info('Forecastiong Job Started...')
		
		except FileNotFoundError:
			os.mkdir(os.getcwd() + '\\logs')
			LOG_FILENAME = '.\\logs\\casas_2_log_' + str(date.today()) + '.log'

			for handler in logging.root.handlers[:]:
				logging.root.removeHandler(handler)
			
			logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG, format= '%(asctime)s : %(levelname)s : %(message)s')
		
		print_('#'*40)
		print_(f'''{'#'*17} {response.status} {'#'*18}''')
		print_('#'*40)

		categories = response.xpath('//*[@id= "main_menu"]/ul/li/a/@href').extract()

		categories = [link for link in categories if '#' not in link and 'https' not in link]

		print()


		for n in categories:
			print(n)

		print()

		category = categories[N_category]

		sub_categories = response.xpath('//*[@id= "main_menu"]/ul/li/a[contains(@href, "'+category+'")]').xpath('..//ul/li/a/@href').extract()
		sub_categories = ['https://www.fincaraiz.com.co'+link for link in sub_categories if '#' not in link and 'https' not in link]

		# for x in sub_categories:
		# 	print(x)

		current_sub_categories = sub_categories[4:8] ################################################ sub categories
		
		for i in current_sub_categories:
			print(i)

		#sleep(120) ######################## OJO !!

		n_sub_cat = 0

		check_connection()
		yield Request(url= response.url,
					  callback= self.initial_parse,
					  meta= {'current_sub_categories': current_sub_categories,
							 'n_sub_cat': n_sub_cat},
					  dont_filter= True)


	def initial_parse(self, response):
		current_sub_categories = response.meta['current_sub_categories']
		n_sub_cat = response.meta['n_sub_cat']

		sub_category = current_sub_categories[n_sub_cat]

		try:
			driver = webdriver.Firefox()
			print_('Se logro abrir el explorador')
			sleep(3)

			check_connection()
			print_('Antes de el get de seleniumwire')
			driver.get(sub_category)
			print_('Despues de el get de seleniumwire')
			sleep(3)

			for request in driver.requests:
				if 'Find?semantic' in str(request.url) and 'text/xml' in request.response.headers['Content-Type']:
					print(request)
					api = str(request)

			driver.quit()

		except:
			try: driver.quit()
			except: pass

			sleep(10)

			driver = webdriver.Firefox()
			print_('Se logro abrir el explorador')
			sleep(3)

			check_connection()
			print_('Antes de el get de seleniumwire')
			driver.get(sub_category)
			print_('Despues de el get de seleniumwire')
			sleep(3)

			for request in driver.requests:
				if 'Find?semantic' in str(request.url) and 'text/xml' in request.response.headers['Content-Type']:
					print(request)
					api = str(request)

			driver.quit()




		try:
			key = api.split('=ad=', 1)[1].split('&', 1)[0]

			sub_part_1 = key.split('|', 2)[0] + '|'
			
			pag_key = int(key.split('|', 2)[1])

			sub_part_3 = '|' + key.split('|', 2)[-1]

		except:
			print_(api)
			print_('OJO HUBO UN ERROR EN LA CLAVE PARA EL API, REVISAR !!')

		url_parts = {
					'1ra_parte': 'https://www.fincaraiz.com.co/WebServices/Property.asmx/Find?semantic=ad=',
					'key_sub1': sub_part_1,
					'key_pag': 	pag_key,
					'key_sub3': sub_part_3,
					'2da_parte': '&semanticReference=ad=',
					'3ra_parte': '&dataTypeResponse=JSON&ControlLocations=LocationsBreadcrumbV2&filterHomeIsCalled=0'
					}

		all_data_list = []
		trys = 0
		trys_per_cache = 0

		check_connection()
		yield Request(url= url_parts['1ra_parte']+ 
						   url_parts['key_sub1']+
						   str(url_parts['key_pag'])+
						   url_parts['key_sub3']+
						   url_parts['2da_parte']+
						   url_parts['key_sub1']+
						   str(url_parts['key_pag'])+
						   url_parts['key_sub3']+
						   url_parts['3ra_parte'],
					  callback= self.first_parse,
					  meta= {'url_parts': url_parts,
							 'sub_category': sub_category,
							 'all_data_list': all_data_list,
							 'trys': trys,
							 'trys_per_cache': trys_per_cache,
							 'current_sub_categories': current_sub_categories,
							 'n_sub_cat': n_sub_cat},
					  headers= {'X-Requested-With':'XMLHttpRequest'})



	def response_500_parse(self, response):
		n_retry = response.meta['n_retry']

		if n_retry['tag'] == 'first':

			url_parts = response.meta['url_parts']
			sub_category = response.meta['sub_category']
			all_data_list = response.meta['all_data_list']
			trys = response.meta['trys']
			trys_per_cache = response.meta['trys_per_cache']

			current_sub_categories = response.meta['current_sub_categories']
			n_sub_cat = response.meta['n_sub_cat']

			yield Request(url= response.url,
						  callback= self.first_parse,
						  meta= {'url_parts': url_parts,
								 'sub_category': sub_category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache,
								 'current_sub_categories': current_sub_categories,
								 'n_sub_cat': n_sub_cat,
								 'n_retry': n_retry,
								 'download_timeout': 100},
						  headers= {'X-Requested-With':'XMLHttpRequest'},
						  dont_filter= True)

		elif n_retry['tag'] == 'main':

			url_parts = response.meta['url_parts']
			sub_category = response.meta['sub_category']
			all_data_list = response.meta['all_data_list']
			trys = response.meta['trys']
			trys_per_cache = response.meta['trys_per_cache']
			cache_url = response.meta['cache_url']
			pag_results = response.meta['pag_results']
			n_project = response.meta['n_project']

			current_sub_categories = response.meta['current_sub_categories']
			n_sub_cat = response.meta['n_sub_cat']	

			yield Request(url= response.url,
						  callback= self.main_parse,
						  meta= {'url_parts': url_parts,
								 'sub_category': sub_category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache,
								 'cache_url': cache_url,
								 'pag_results': pag_results,
								 'n_project': n_project,
								 'current_sub_categories': current_sub_categories,
								 'n_sub_cat': n_sub_cat,
								 'n_retry': n_retry,
								 'download_timeout': 100},
						  dont_filter= True)



	def first_parse(self, response):
		url_parts = response.meta['url_parts']
		sub_category = response.meta['sub_category']
		all_data_list = response.meta['all_data_list']
		trys = response.meta['trys']
		trys_per_cache = response.meta['trys_per_cache']

		current_sub_categories = response.meta['current_sub_categories']
		n_sub_cat = response.meta['n_sub_cat']

		print_()
		print_('#'*40)
		print_(f'''{'#'*17} {response.status} {'#'*18}''')
		print_('#'*40)

		print(type(response.body))
		response_body = response.body.decode('utf-8')
		print_()

		if response.status == 500:
			print_('antes de ver el n_retry')
			try: n_retry = response.meta['n_retry']
			except: n_retry = {'count': 1, 'tag': 'first'} 

			print_('pasó de ver el n_retry, antes de entrar al if')
			if n_retry['count'] < 10:

				print_('entró al if, antes de dormir')
				sleep(2**n_retry['count'])
				print_('ya durmio, antes de aumentar el n_retry')
				n_retry['count'] = n_retry['count'] + 1
				print_('ya aumento el n_retry, auntes de asignar la url_500_response')

				url_500_response = url_parts['1ra_parte']+ url_parts['key_sub1']+str(url_parts['key_pag'])+url_parts['key_sub3']+url_parts['2da_parte']+url_parts['key_sub1']+str(url_parts['key_pag'])+url_parts['key_sub3']+url_parts['3ra_parte']

				print_('ya se asignó la url_500_response, antes del check_connection')				
				check_connection()	

				print_('ya ejecuto el check_connection, antes del request para volver a iniciar')
				n = 0
				try:
					yield Request(url= url_500_response,
								  callback= self.response_500_parse,
								  meta= {'url_parts': url_parts,
										 'sub_category': sub_category,
										 'all_data_list': all_data_list,
										 'trys': trys,
										 'trys_per_cache': trys_per_cache,
										 'current_sub_categories': current_sub_categories,
										 'n_sub_cat': n_sub_cat,
										 'n_retry': n_retry,
										 'download_timeout': 5},
								  headers= {'X-Requested-With':'XMLHttpRequest'},
								  dont_filter= True)
				except:
					print_('Se levanto el error, se va a volver a intentar')
					sleep(5)
					yield Request(url= url_500_response,
								  callback= self.response_500_parse,
								  meta= {'url_parts': url_parts,
										 'sub_category': sub_category,
										 'all_data_list': all_data_list,
										 'trys': trys,
										 'trys_per_cache': trys_per_cache,
										 'current_sub_categories': current_sub_categories,
										 'n_sub_cat': n_sub_cat,
										 'n_retry': n_retry,
										 'download_timeout': 5},
								  headers= {'X-Requested-With':'XMLHttpRequest'},
								  dont_filter= True)
				

			else:

				print_('entra al else, antes de reinicial el n_retry')
				n_retry = {'count': 1, 'tag': 'first'}
				
				print_('reinicio el n_retry, antes de avanzar de pag')
				
				url_parts['key_pag'] = url_parts['key_pag'] + 1
				
				print_('ya se incrememto la pag, antes de asignar url_500_response')

				url_500_response = url_parts['1ra_parte']+ url_parts['key_sub1']+str(url_parts['key_pag'])+url_parts['key_sub3']+url_parts['2da_parte']+url_parts['key_sub1']+str(url_parts['key_pag'])+url_parts['key_sub3']+url_parts['3ra_parte']

				print_('ya se asignó la url_500_response, antes del check_connection')				
				check_connection()	

				print_('ya ejecuto el check_connection, antes del request para volver a iniciar')
				yield Request(url= url_500_response,
							  callback= self.response_500_parse,
							  meta= {'url_parts': url_parts,
									 'sub_category': sub_category,
									 'all_data_list': all_data_list,
									 'trys': trys,
									 'trys_per_cache': trys_per_cache,
									 'current_sub_categories': current_sub_categories,
									 'n_sub_cat': n_sub_cat,
									 'n_retry': n_retry},
							  headers= {'X-Requested-With':'XMLHttpRequest'},
							  dont_filter= True)
				print_('deberia ser despues del request')


		try:
			raw_databody = str(response_body).split('<string xmlns="http://tempuri.org/">')[-1].split('[facets]')[0]
			raw_databody = raw_databody.split(':')

			raw_databody_proc = ''

			for sec in raw_databody:
				if '{' in sec and '"' not in sec:
					if '[' in sec:

						sec_proc = '[{"' + sec.split('{')[-1] + '":'
						raw_databody_proc = raw_databody_proc + sec_proc

					else:

						sec_proc = sec.replace('{', '{"') + '":'
						raw_databody_proc = raw_databody_proc + sec_proc


				elif '{' not in sec and '}' not in sec:
					sec_proc = sec[::-1].split(',', 1)

					sec_proc = ':"' + sec_proc[0] + '",' + sec_proc[-1]

					sec_proc = sec_proc[::-1]
					raw_databody_proc = raw_databody_proc + sec_proc
					

				elif '{' in sec and '}' in sec:
					sec_proc = sec[::-1].split(',', 1)
					

					sec_proc = ':"' + sec_proc[0].replace('{', '"{,') + sec_proc[-1]
					
					sec_proc = sec_proc[::-1]
					raw_databody_proc = raw_databody_proc + sec_proc



				else:
					if 'info' in sec:
						raw_databody_proc = raw_databody_proc + sec + ':'	
					else:
						raw_databody_proc = raw_databody_proc + sec

		except:
			print_('OJO, TAL PARECE QUE HUBO UN ERROR A LA HORA DE TRATAR EL CUERPO DE JSON, POSIBLE ERROR EN LA RESPUESTA')



		raw_databody_proc= str.encode(raw_databody_proc)

		data_json = loads(raw_databody_proc)

		cache_url = response.url

		check_connection()
		yield Request(url= sub_category,
					  callback= self.second_parse,
					  meta= {'url_parts': url_parts,
							 'sub_category': sub_category,
							 'all_data_list': all_data_list,
							 'trys': trys,
							 'trys_per_cache': trys_per_cache,
							 'data_json': data_json,
							 'cache_url': cache_url,
							 'current_sub_categories': current_sub_categories,
							 'n_sub_cat': n_sub_cat},
					  dont_filter= True)




	def second_parse(self, response):

		url_parts = response.meta['url_parts']
		sub_category = response.meta['sub_category']
		all_data_list = response.meta['all_data_list']
		trys = response.meta['trys']
		trys_per_cache = response.meta['trys_per_cache']
		data_json = response.meta['data_json']
		cache_url = response.meta['cache_url']

		current_sub_categories = response.meta['current_sub_categories']
		n_sub_cat = response.meta['n_sub_cat']

		print_('Second parse')
		print_('#'*40)
		print_(f'''{'#'*17} {response.status} {'#'*18}''')
		print_('#'*40)

		#print_(data_json)

		inmuebles = data_json['data']

		test = 0

		pag_results = []

		for inmueble in inmuebles:
			
			inmu_id = inmueble['AdvertId']
			cliente_id = inmueble['ClientId']
			cliente_name = inmueble['ClientName']


			title = inmueble['Title']
			price= inmueble['FormatedPrice']
			
			location_1 = inmueble['Location1']
			location_2 = inmueble['Location2']
			location_3 = inmueble['Location3']

			area_formated = inmueble['FormatedSurface']
			area = inmueble['Area']

			tipo_proj = inmueble['Category1']
			inmu_status = inmueble['AdvertType'] 
			zone = inmueble['Neighborhood']
			
			rooms = inmueble['Rooms']

			image_url = inmueble['PhotoUrl']

			inmu_url = 'https://www.fincaraiz.com.co' + inmueble['SemanticText']


			data_test = {
						 'inmu_id': inmu_id,
						 'title': title,
						 'price': price
						}

			if data_test not in all_data_list:

				test += 1

				print_(f'''
						### pag {url_parts['key_pag']} ###

						ID: {data_test['inmu_id']}
						Titulo: {data_test['title']}
						Price {data_test['price']}
					
						''')


				projects_data =  { 
									'inmu_id': inmu_id,
									'cliente_id': cliente_id,
									'cliente_name': cliente_name,
									'title': title,
									'price': price,
									'location_1': location_1,
									'location_2': location_2,
									'location_3': location_3,
									'area_formated': area_formated,
									'area': area,
									'tipo_proj': tipo_proj,
									'inmu_status': inmu_status,
									'zone': zone,
									'rooms': rooms,
									'image_url': image_url,
									'inmu_url': inmu_url
								  }

				pag_results.append(projects_data)
				
				all_data_list.append(data_test)


			




		if test != 0:
			trys = 0


			n_project = 0

			ini_url_pag = pag_results[n_project]['inmu_url']

			check_connection()
			yield Request(url= ini_url_pag,
						  callback= self.main_parse,
						  meta= {'url_parts': url_parts,
								 'sub_category': sub_category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache,
								 'cache_url': cache_url,
								 'pag_results': pag_results,
								 'n_project': n_project,
								 'current_sub_categories': current_sub_categories,
								 'n_sub_cat': n_sub_cat},
						  dont_filter= True)



		elif test == 0 and trys <= 50:
			trys += 1
			trys_per_cache = 0

			url_parts['key_pag'] = url_parts['key_pag'] + 1

			check_connection()
			yield Request(url= url_parts['1ra_parte']+ 
							   url_parts['key_sub1']+
							   str(url_parts['key_pag'])+
							   url_parts['key_sub3']+
							   url_parts['2da_parte']+
							   url_parts['key_sub1']+
							   str(url_parts['key_pag'])+
							   url_parts['key_sub3']+
							   url_parts['3ra_parte'],
						  callback= self.first_parse,
						  meta= {'url_parts': url_parts,
								 'sub_category': sub_category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache,
								 'current_sub_categories': current_sub_categories,
								 'n_sub_cat': n_sub_cat},
						  headers= {'X-Requested-With':'XMLHttpRequest'},
						  dont_filter= True)

		elif n_sub_cat < len(current_sub_categories)-1:
			n_sub_cat += 1
			global start_urls
			check_connection()
			yield Request(url= 'https://www.fincaraiz.com.co/',
						  callback= self.initial_parse,
						  meta= {'current_sub_categories': current_sub_categories,
								 'n_sub_cat': n_sub_cat},
						  dont_filter= True)

		else:

			print_('POR AHORA TERMINAMOS !!')





	def main_parse(self, response):

		url_parts = response.meta['url_parts']
		sub_category = response.meta['sub_category']
		all_data_list = response.meta['all_data_list']
		trys = response.meta['trys']
		trys_per_cache = response.meta['trys_per_cache']
		cache_url = response.meta['cache_url']
		pag_results = response.meta['pag_results']
		n_project = response.meta['n_project']

		current_sub_categories = response.meta['current_sub_categories']
		n_sub_cat = response.meta['n_sub_cat']		

		# Siempre cuando entre aca hara llamado al cache !!  no lo olvide



		inmu_id= pag_results[n_project]['inmu_id']
		cliente_id= pag_results[n_project]['cliente_id']
		cliente_name= pag_results[n_project]['cliente_name']
		title= pag_results[n_project]['title']
		price= pag_results[n_project]['price']
		location_1= pag_results[n_project]['location_1']
		location_2= pag_results[n_project]['location_2']
		location_3= pag_results[n_project]['location_3']
		area_formated= pag_results[n_project]['area_formated']
		area= pag_results[n_project]['area']
		tipo_proj= pag_results[n_project]['tipo_proj']
		inmu_status= pag_results[n_project]['inmu_status']
		zone= pag_results[n_project]['zone']
		rooms= pag_results[n_project]['rooms']

		image_url= pag_results[n_project]['image_url']
		inmu_url= pag_results[n_project]['inmu_url']

		

		print_()
		print_('main parse')
		print_('#'*40)
		print_(f'''{'#'*17} {response.status} {'#'*18}''')
		print_('#'*40)
		print_(response.url)
		print_()


		if response.status == 500:
			print_('intentando hacer una conexion a '+ inmu_url)
			print_('antes de ver el n_retry')
			try: n_retry = response.meta['n_retry']
			except: n_retry = {'count': 1, 'tag': 'main'}
			print_('pasó de ver el n_retry, antes de entrar al if')
			if n_retry['count'] < 10:
				print_('entró al if, antes de dormir')
				sleep(2**n_retry['count'])
				print_('ya durmio, antes de aumentar el n_retry')
				n_retry['count'] = n_retry['count'] + 1
				print_('se aumento el n_retry, antes de check_connection')
				check_connection()
				print_('se ejecuto check_connection, antes del request')

				try:
					yield Request(url= inmu_url,
								  callback= self.response_500_parse,
								  meta= {'url_parts': url_parts,
										 'sub_category': sub_category,
										 'all_data_list': all_data_list,
										 'trys': trys,
										 'trys_per_cache': trys_per_cache,
										 'cache_url': cache_url,
										 'pag_results': pag_results,
										 'n_project': n_project,
										 'current_sub_categories': current_sub_categories,
										 'n_sub_cat': n_sub_cat,
										 'n_retry': n_retry,
										 'download_timeout': 5},
								  dont_filter= True) 
				except:
					print_('Se levanto el error, se va a volver a intentar')
					sleep(5)
					yield Request(url= inmu_url,
								  callback= self.response_500_parse,
								  meta= {'url_parts': url_parts,
										 'sub_category': sub_category,
										 'all_data_list': all_data_list,
										 'trys': trys,
										 'trys_per_cache': trys_per_cache,
										 'cache_url': cache_url,
										 'pag_results': pag_results,
										 'n_project': n_project,
										 'current_sub_categories': current_sub_categories,
										 'n_sub_cat': n_sub_cat,
										 'n_retry': n_retry,
										 'download_timeout': 5},
								  dont_filter= True)

			else:
				print_('entra al else n_retry = '+str(n_retry))
				n_retry = {'count': 1, 'tag': 'main'}

				n_project +=1

				next_url_pag = pag_results[n_project]['inmu_url']
				print_('despues de reiniciar el n_retry, aumetar el n_project y obtener la siguiente url, se va a check_connection')
				check_connection()
				print_('se ejecuto check_connection, antes del request')	
				yield Request(url= next_url_pag,
							  callback= self.main_parse,
							  meta= {'url_parts': url_parts,
									 'sub_category': sub_category,
									 'all_data_list': all_data_list,
									 'trys': trys,
									 'trys_per_cache': trys_per_cache,
									 'cache_url': cache_url,
									 'pag_results': pag_results,
									 'n_project': n_project,
									 'current_sub_categories': current_sub_categories,
									 'n_sub_cat': n_sub_cat,
									 'n_retry': n_retry},
							  dont_filter= True)
				print_('despues del request en el else')




		#print(response.body)

		rooms_details = response.xpath('//*[@class= "advertRooms"]/text()').extract()
		
		if rooms_details != [] and rooms_details != '' and rooms_details != None:
			rooms_details = [x for x in rooms_details if x.strip() != '']
			rooms_details = rooms_details[-1]

			rooms_details = rooms_details.replace('\n', '')
			rooms_details = rooms_details.replace('\r', '')
			rooms_details = rooms_details.strip()


		restrooms_details = response.xpath('//*[@class= "advertBaths"]/text()').extract()

		if restrooms_details != [] and restrooms_details != '' and restrooms_details != None:
			restrooms_details = [x for x in restrooms_details if x.strip() != '']
			restrooms_details = restrooms_details[-1]

			restrooms_details = restrooms_details.replace('\n', '')
			restrooms_details = restrooms_details.replace('\r', '')
			restrooms_details = restrooms_details.strip()


		garages_details = response.xpath('//*[@class= "advertGarages"]/text()').extract()

		if garages_details != [] and garages_details != '' and garages_details != None:
			garages_details = [x for x in garages_details if x.strip() != '']
			garages_details = garages_details[-1]

			garages_details = garages_details.replace('\n', '')
			garages_details = garages_details.replace('\r', '')
			garages_details = garages_details.strip()


		area_priv = response.xpath('//*[@class= "row features_2 "]/ul//*[contains(text(), "Área privada:")]').xpath('../text()').extract_first()

		if area_priv != None:
			area_priv = area_priv.strip()


		area_cons = response.xpath('//*[@class= "row features_2 "]/ul//*[contains(text(), "Área Const.:")]').xpath('../text()').extract_first()

		if area_cons != None:
			area_cons = area_cons.strip()


		estrato = response.xpath('//*[@class= "row features_2 "]/ul//*[contains(text(), "Estrato:")]').xpath('../text()').extract_first()

		if estrato != None:
			estrato = estrato.strip()

		

		antiguedad = response.xpath('//*[@class= "row features_2 "]/ul//*[contains(text(), "Antigüedad:")]').xpath('../text()').extract_first()

		if antiguedad != None:
			antiguedad = antiguedad.strip()



		n_piso = response.xpath('//*[@class= "row features_2 "]/ul//*[contains(text(), "Piso No:")]').xpath('../text()').extract_first()

		if n_piso != None:
			n_piso = n_piso.strip()


		admon = response.xpath('//*[@class= "row features_2 "]/ul//*[contains(text(), "Admón:")]').xpath('../text()').extract_first()

		if admon != None:
			admon = admon.strip()



		caracteristicas = response.xpath('//*[@class = "features_3"]/ul[@class= "InitialUL"]/li/text()').extract()

		if caracteristicas != []:

			caracteristicas = [caract.strip() for caract in caracteristicas] 
			caracteristicas = str(caracteristicas)			


		last_update = response.xpath('//*[@class= "box_content row historyAdvert"]//*[contains(text(), "Actualizado:")]').xpath('../span/text()').extract_first()

		if last_update != None:
			last_update = last_update.strip()			

		id_details = response.xpath('//h2[@class= "description"]/span/b/text()').extract_first()

		while True:
			try:
				if id_details != None:

					id_details = id_details.strip()

					check_connection()
					views = get('https://www.fincaraiz.com.co/WebServices/Statistics.asmx/GetAdvertVisits?idAdvert='+id_details+'&idASource=40&idType=1001')

					views_sel = Selector(text= views.text)
					views = views_sel.xpath('//double/text()').extract_first()
					break
			except:
				continue


		yield {
				'inmu_id': inmu_id,
				'id_details': id_details,
				'cliente_id': cliente_id,
				'cliente_name': cliente_name,
				'title': title,
				'price': price,
				'admon': admon,
				'location_1': location_1,
				'location_2': location_2,
				'location_3': location_3,
				'area_formated': area_formated,
				'area': area,
				'area_priv': area_priv,
				'area_cons': area_cons,
				'tipo_proj': tipo_proj,
				'inmu_status': inmu_status,
				'antiguedad': antiguedad,
				'n_piso': n_piso,
				'estrato': estrato,
				'zone': zone,
				'rooms': rooms,
				'rooms_details': rooms_details,
				'restrooms_details': restrooms_details,
				'garages_details': garages_details,
				'last_update': last_update,
				'views': views,
				'caracteristicas': caracteristicas,
				'image_url': image_url,
				'inmu_url': inmu_url
			  }


		if n_project < len(pag_results)-1:
			n_project +=1

			next_url_pag = pag_results[n_project]['inmu_url']

			check_connection()
			yield Request(url= next_url_pag,
						  callback= self.main_parse,
						  meta= {'url_parts': url_parts,
								 'sub_category': sub_category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache,
								 'cache_url': cache_url,
								 'pag_results': pag_results,
								 'n_project': n_project,
								 'current_sub_categories': current_sub_categories,
								 'n_sub_cat': n_sub_cat},
						  dont_filter= True)

		else:
			check_connection()
			yield Request(url= cache_url,
						  callback= self.first_parse,
						  meta= {'url_parts': url_parts,
								 'sub_category': sub_category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache,
								 'current_sub_categories': current_sub_categories,
								 'n_sub_cat': n_sub_cat},
						  headers= {'X-Requested-With':'XMLHttpRequest'},
						  dont_filter= True)