from scrapy import Spider
from scrapy.http import Request
from scrapy.selector import Selector

from seleniumwire import webdriver
from requests import get

from json import loads


from time import sleep


import os, logging
from datetime import date


N_category = 1



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


class FincaraizScrapingApartamentos1Spider(Spider):
	name = 'fincaraiz_scraping_apartamentos_1'
	allowed_domains = ['fincaraiz.com.co']
	start_urls = ['https://www.fincaraiz.com.co/']

	def parse(self, response):

		try:
			LOG_FILENAME = '.\\logs\\aptos_1_log_' + str(date.today()) + '.log'

			for handler in logging.root.handlers[:]:
				logging.root.removeHandler(handler)
			
			logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG, format= '%(asctime)s : %(levelname)s : %(message)s')    
			logging.info('Forecastiong Job Started...')
		
		except FileNotFoundError:
			os.mkdir(os.getcwd() + '\\logs')
			LOG_FILENAME = '.\\logs\\aptos_1_log_' + str(date.today()) + '.log'

			for handler in logging.root.handlers[:]:
				logging.root.removeHandler(handler)
			
			logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG, format= '%(asctime)s : %(levelname)s : %(message)s')
		
		print_('#'*40)
		print_(f'''{'#'*17} {response.status} {'#'*18}''')
		print_('#'*40)

		categories = response.xpath('//*[@id= "main_menu"]/ul/li/a/@href').extract()

		categories = ['https://www.fincaraiz.com.co'+link for link in categories if '#' not in link and 'https' not in link]

		print()

		for n in categories:
			print(n)

		print()

		category = categories[N_category]

		driver = webdriver.Firefox()
		sleep(3)

		driver.get(category)
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
							 'category': category,
							 'all_data_list': all_data_list,
							 'trys': trys,
							 'trys_per_cache': trys_per_cache},
					  headers= {'X-Requested-With':'XMLHttpRequest'})

		# url_ = 'https://www.fincaraiz.com.co/WebServices/Property.asmx/Find?semantic=ad=30|2||||1|||||||||||||||||||||||0||||||||||&semanticReference=ad=30|2||||1|||||||||||||||||||||||0||||||||||&dataTypeResponse=JSON&ControlLocations=LocationsBreadcrumbV2&filterHomeIsCalled=0'
		# yield Request(url= url_,
		# 			  callback= self.first_parse,
		# 			  #meta= {'url_parts': url_parts},
		# 			  meta= {'category':category},
		# 			  headers= {'X-Requested-With':'XMLHttpRequest',
		# 			  			'Accept': 'application/xml, text/xml, */*; q=0.01'})
















	def first_parse(self, response):
		url_parts = response.meta['url_parts']
		category = response.meta['category']
		all_data_list = response.meta['all_data_list']
		trys = response.meta['trys']
		trys_per_cache = response.meta['trys_per_cache']

		print_()
		print_('#'*40)
		print_(f'''{'#'*17} {response.status} {'#'*18}''')
		print_('#'*40)

		print(type(response.body))
		response_body = response.body.decode('utf-8')
		print_()

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
		#print_('De aqui pa abajo si hay json')
		#print_(loads(raw_databody_proc))

		data_json = loads(raw_databody_proc)

		cache_url = response.url


		yield Request(url= category,
					  callback= self.second_parse,
					  meta= {'url_parts': url_parts,
							 'category': category,
							 'all_data_list': all_data_list,
							 'trys': trys,
							 'trys_per_cache': trys_per_cache,
							 'data_json': data_json,
							 'cache_url': cache_url},
					  dont_filter= True)




	def second_parse(self, response):

		url_parts = response.meta['url_parts']
		category = response.meta['category']
		all_data_list = response.meta['all_data_list']
		trys = response.meta['trys']
		trys_per_cache = response.meta['trys_per_cache']
		data_json = response.meta['data_json']
		cache_url = response.meta['cache_url']


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

			yield Request(url= ini_url_pag,
						  callback= self.main_parse,
						  meta= {'url_parts': url_parts,
								 'category': category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache,
								 'cache_url': cache_url,
								 'pag_results': pag_results,
								 'n_project': n_project},
						  dont_filter= True)


		# elif test == 0 and trys_per_cache < 10:
		# 		trys_per_cache += 1
		# 		sleep(1)
		# 		print_()
		# 		print_(trys_per_cache)
		# 		print_()

		# 		yield Request(url= cache_url,
		# 				  callback= self.first_parse,
		# 				  meta= {'url_parts': url_parts,
		# 						 'category': category,
		# 						 'all_data_list': all_data_list,
		# 						 'trys': trys,
		# 						 'trys_per_cache': trys_per_cache},
		# 				  headers= {'X-Requested-With':'XMLHttpRequest'},
		# 				  dont_filter= True)



		elif test == 0 and trys <= 50:
			trys += 1
			trys_per_cache = 0

			url_parts['key_pag'] = url_parts['key_pag'] + 1

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
								 'category': category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache},
						  headers= {'X-Requested-With':'XMLHttpRequest'},
						  dont_filter= True)


		else:

			print_('POR AHORA TERMINAMOS !!')





	def main_parse(self, response):

		url_parts = response.meta['url_parts']
		category = response.meta['category']
		all_data_list = response.meta['all_data_list']
		trys = response.meta['trys']
		trys_per_cache = response.meta['trys_per_cache']
		cache_url = response.meta['cache_url']
		pag_results = response.meta['pag_results']
		n_project = response.meta['n_project']

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

		if id_details != None:

			id_details = id_details.strip()

			views = get('https://www.fincaraiz.com.co/WebServices/Statistics.asmx/GetAdvertVisits?idAdvert='+id_details+'&idASource=40&idType=1001')

			views_sel = Selector(text= views.text)
			views = views_sel.xpath('//double/text()').extract_first()



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

			yield Request(url= next_url_pag,
						  callback= self.main_parse,
						  meta= {'url_parts': url_parts,
								 'category': category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache,
								 'cache_url': cache_url,
								 'pag_results': pag_results,
								 'n_project': n_project},
						  dont_filter= True)

		else:
			yield Request(url= cache_url,
						  callback= self.first_parse,
						  meta= {'url_parts': url_parts,
								 'category': category,
								 'all_data_list': all_data_list,
								 'trys': trys,
								 'trys_per_cache': trys_per_cache},
						  headers= {'X-Requested-With':'XMLHttpRequest'},
						  dont_filter= True)	
