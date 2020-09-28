from scrapy import Spider
from scrapy.http import Request

from json import loads

from time import sleep

import os, logging

from datetime import date

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
			sleep(120)
			continue


check_connection()

N_categoria = 0 # Como son varias categorias construidas bajo la misma clase, se optó por acceder a cada una de ellas de manera separada para hacer más simple el codigo (hasta el dia 28/08/2020 son 4 categorias)

API_key = 'P1MfFHfQMOtL16Zpg36NcntJYCLFm8FqFfudnavl' # Toca estar pendiente si es dinamica cada cierto tiempo, por eso se deja apartarda para facilitar su manejo.


class MetrocScrapingAptosVentaSpider(Spider):
	name = 'metroc_scraping_aptos_venta'
	allowed_domains = ['metrocuadrado.com']
	start_urls = ['https://www.metrocuadrado.com/']

	
	def print_(self, message = ' ', type_ = 'info'):

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


	def parse(self, response):

		try:
			LOG_FILENAME = '.\\logs\\aptos_venta_log_' + str(date.today()) + '.log'

			for handler in logging.root.handlers[:]:
				logging.root.removeHandler(handler)
			
			logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG, format= '%(asctime)s : %(levelname)s : %(message)s')    
			logging.info('Forecastiong Job Started...')
		
		except FileNotFoundError:
			os.mkdir(os.getcwd() + '\\logs')
			LOG_FILENAME = '.\\logs\\aptos_venta_log_' + str(date.today()) + '.log'

			for handler in logging.root.handlers[:]:
				logging.root.removeHandler(handler)
			
			logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG, format= '%(asctime)s : %(levelname)s : %(message)s')    
		

		print()
		print('Entra aca 1')
		print()
		
		aptos_links = response.xpath('//*[@class= "box-list"]')[N_categoria].xpath('.//li//a/@href').extract()
		aptos_links = [link.replace('http', 'https') for link in aptos_links]

		#aptos_links = [print(link) for link in aptos_links if 'arriendo' not in link]

		data_links = []

		for url in aptos_links:
			items = {}
			url = url.split('.com')[-1].split('/')

			for ind, info in enumerate(url):
				if info == '':
					url.pop(ind)

			items['inmu_'] = url[0]
			items['type_'] = url[1]
			items['loc_'] = url[-1]

			data_links.append(items)

		n_cat = 0 ################################################################################# ojo !!

		check_connection()
		yield Request(url= response.url,
					  callback= self.first_parse,
					  meta= {'data_links': data_links,
							 'n_cat': n_cat,
							 'aptos_links': aptos_links},
					  dont_filter= True)

	def first_parse(self, response):
		data_links = response.meta['data_links']
		n_cat = response.meta['n_cat']
		aptos_links = response.meta['aptos_links']
		n_from = 0 ###################################################################### ojo     			

		cat_link = aptos_links[n_cat]
		data_link = data_links[n_cat]

		print(data_link)

		inmu_ = data_link['inmu_']
		type_ = data_link['type_']
		loc_ = data_link['loc_']

		api_link = 'https://www.metrocuadrado.com/rest-search/search?seo=/'+inmu_+'/'+type_+'/'+loc_+'/&from='+str(n_from)+'&size=300'  

		n = 0
		trys = 0

		check_connection()
		yield Request(
					  headers= {'Accept': 'application/json, text/plain, */*',
								'Accept-Encoding': 'gzip, deflate, br',
								'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
								'Connection': 'keep-alive',
								'DNT': '1',
								'Host': 'www.metrocuadrado.com',
								'Upgrade-Insecure-Requests': '1',
								'Referer': cat_link,
								'Pragma': 'no-cache',
								'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0',
								'X-Api-Key': API_key,
								'X-Requested-With': 'XMLHttpRequest'},
					  callback= self.main_parse,
					  meta= {'data_links': data_links,
							 'n_cat': n_cat,
							 'n_from': n_from,
							 'data_link': data_link,
							 'cat_link': cat_link,
							 'aptos_links':aptos_links,
							 'trys': trys,
							 'n': n},
					  
					  url= api_link,
					  
					  dont_filter= True
					  
					  )


	def partial_parse(self, response):
		data_links = response.meta['data_links']
		n_cat = response.meta['n_cat']
		n_from = response.meta['n_from']
		data_link = response.meta['data_link']
		cat_link = response.meta['cat_link']
		aptos_links = response.meta['aptos_links']
		trys = response.meta['trys']

		cache_n_from = response.meta['cache_n_from']

		n = response.meta['n']

		inmu_ = data_link['inmu_'].replace('s', '')
		type_ = data_link['type_']
		loc_ = data_link['loc_']

		api_link = 'https://www.metrocuadrado.com/rest-search/search?realEstateTypeList='+inmu_+'&realEstateBusinessList='+type_+'&city='+loc_+'&from='  
		
		check_connection()
		yield Request(
					  headers= {'Accept': 'application/json, text/plain, */*',
								'Accept-Encoding': 'gzip, deflate, br',
								'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
								'Connection': 'keep-alive',
								'DNT': '1',
								'Host': 'www.metrocuadrado.com',
								'Upgrade-Insecure-Requests': '1',
								'Referer': cat_link,
								'Pragma': 'no-cache',
								'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0',
								'X-Api-Key': API_key,
								'X-Requested-With': 'XMLHttpRequest'},

					  callback= self.main_parse,

					  meta= {'data_links': data_links,
							 'n_cat': n_cat,
							 'n_from': n_from,
							 'data_link': data_link,
							 'cat_link': cat_link,
							 'aptos_links':aptos_links,
							 'cache_n_from': cache_n_from,
							 'trys': trys,
							 'n': n},

					  url= api_link+str(n_from)+'&size=300',
					  dont_filter= True)





	def main_parse(self, response):

		data_links = response.meta['data_links']
		n_cat = response.meta['n_cat']
		n_from = response.meta['n_from']
		data_link = response.meta['data_link']
		cat_link = response.meta['cat_link']
		aptos_links = response.meta['aptos_links']

		trys = response.meta['trys']

		try: cache_n_from = response.meta['cache_n_from']
		except: cache_n_from = 0 


		n = response.meta['n']

		self.print_()
		self.print_(response.url)
		self.print_()
		print(response.status, type(response.status), 500 == response.status)
		print()



		jsonresponse = loads(response.text)
		try: test = jsonresponse['results']
		except: test = []
		#print(jsonresponse)
		self.print_()
		self.print_('CATEGORIA N' + str(n_cat+1)+ '-------------------> count')
		self.print_()

		#sleep(5)

		if response.status == 500 and trys < 5:

			if trys == 0:
				plus = 50
			elif trys == 1:
				plus = 40
			elif trys == 2:
				plus = 30
			elif trys == 3:
				plus = 20
			else:
				plus = 10

			n_from = cache_n_from + plus



			trys +=1
			check_connection()
			yield Request(url= cat_link,
						  callback= self.partial_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								 'aptos_links':aptos_links,
								 'cache_n_from': cache_n_from,
								 'trys': trys,
								 'n': n},
						  dont_filter= True)



		elif test != [] and response.status not in [500]: # and n < 1:

			cache_n_from = n_from
			trys = 0

			n_from += 300
			n += 1

			aptos = jsonresponse['results']

			url_aptos = []
			data_aptos = []
			n_apto = 0
			
			for apto in aptos:
				#data_apto = {}

				id_apto = apto['midinmueble']

				mun_name = apto['mciudad']['nombre']

				cel_num = apto['contactPhone']

				constructor = apto['mnombreconstructor']
				proyecto = apto['mnombreproyecto']
				estado = apto['mestadoinmueble']


				try: zona = apto['mzona']['nombre']
				except: zona = apto['mzona']

				barrio = apto['mbarrio']
				nom_barrio_comun = apto['mnombrecomunbarrio']


				tipo = apto['mtipoinmueble']['nombre']
				title= apto['title']
				tipo_negocio = apto['mtiponegocio']
				val_venta = apto['mvalorventa']
				val_arriendo = apto['mvalorarriendo']


				area = apto['marea']
				n_habs = apto['mnrocuartos']
				n_banos = apto['mnrobanos']
				n_garajes = apto['mnrogarajes']


				image_url = apto['imageLink']

				url = 'https://www.metrocuadrado.com' + apto['link']
				
				data_apto =  { 
							   'id_apto': id_apto,
							   'mun_name': mun_name,
							   'cel_num': cel_num,
						   	   'constructor': constructor,
						   	   'proyecto': proyecto,
						   	   'estado': estado,
						   	   'zona': zona,
						   	   'barrio': barrio,
						   	   'nom_barrio_comun': nom_barrio_comun,
						   	   'tipo': tipo,
						   	   'title': title,
						   	   'tipo_negocio': tipo_negocio,
						   	   'val_venta': val_venta,
						   	   'val_arriendo': val_arriendo,
						   	   'area': area,
						   	   'n_habs': n_habs,
						   	   'n_banos': n_banos,
						   	   'n_garajes': n_garajes,
						   	   'url': url
							}
				data_aptos.append(data_apto)


			url_apto = data_aptos[n_apto]['url']

			print(data_aptos[n_apto])

			check_connection()
			yield Request(url= url_apto,
						  callback= self.details_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								 'aptos_links':aptos_links,
								 'cache_n_from': cache_n_from,
								 'trys': trys, 
								 'data_aptos': data_aptos,
								 'n_apto': n_apto,
								 'n': n},
					  	  dont_filter= True,
					  	  headers= {'Accept': 'application/json, text/plain, */*',
								'Accept-Encoding': 'gzip, deflate, br',
								'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
								'Connection': 'keep-alive',
								'DNT': '1',
								'Host': 'www.metrocuadrado.com',
								'Upgrade-Insecure-Requests': '1',
								'Referer': cat_link,
								'Pragma': 'no-cache',
								'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0',
								'X-Api-Key': API_key,
								'X-Requested-With': 'XMLHttpRequest'})



		elif n_cat < len(aptos_links)- 1:
			n_cat += 1
			check_connection()
			yield Request(url= 'https://www.metrocuadrado.com/',
						  callback= self.first_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'aptos_links': aptos_links},
						  dont_filter= True)
		else:
			#logging.shutdown()
			self.print_('#'*20+' Por ahora terminamos '+'#'*20)




	def details_parse(self, response):
		data_links= response.meta['data_links']
		n_cat= response.meta['n_cat']
		n_from= response.meta['n_from']
		data_link= response.meta['data_link']
		cat_link= response.meta['cat_link']
		aptos_links= response.meta['aptos_links']
		cache_n_from= response.meta['cache_n_from']
		trys= response.meta['trys']
		data_aptos = response.meta['data_aptos']
		n_apto = response.meta['n_apto']

		n= response.meta['n']	

		self.print_()
		self.print_(response.url)
		self.print_()
		#self.print_(response.text)
		self.print_()

		#self.print_(apto)

		id_apto = data_aptos[n_apto]['id_apto']
		mun_name = data_aptos[n_apto]['mun_name']
		cel_num = data_aptos[n_apto]['cel_num']
		constructor = data_aptos[n_apto]['constructor']
		proyecto = data_aptos[n_apto]['proyecto']
		estado = data_aptos[n_apto]['estado']
		zona = data_aptos[n_apto]['zona']
		barrio = data_aptos[n_apto]['barrio']
		nom_barrio_comun = data_aptos[n_apto]['nom_barrio_comun']
		tipo = data_aptos[n_apto]['tipo']
		title = data_aptos[n_apto]['title']
		tipo_negocio = data_aptos[n_apto]['tipo_negocio']
		val_venta = data_aptos[n_apto]['val_venta']
		val_arriendo = data_aptos[n_apto]['val_arriendo']
		area = data_aptos[n_apto]['area']
		n_habs = data_aptos[n_apto]['n_habs']
		n_banos = data_aptos[n_apto]['n_banos']
		n_garajes = data_aptos[n_apto]['n_garajes']
		url = data_aptos[n_apto]['url']

		estrato = response.xpath('//h2[@class= "H2-kplljn-0 fxUScU card-text"]/text()').extract_first()
		area_const = response.xpath('//*[@class= "Col-sc-14ninbu-0 lfGZKA mb-3 pb-1 col-12 col-lg-3"]//h3[contains(text(), "Área construida")]').xpath('..//p/text()').extract_first()
		area_priv = response.xpath('//*[@class= "Col-sc-14ninbu-0 lfGZKA mb-3 pb-1 col-12 col-lg-3"]//h3[contains(text(), "Área privada")]').xpath('..//p/text()').extract_first()
		antiguedad = response.xpath('//*[@class= "Col-sc-14ninbu-0 lfGZKA mb-3 pb-1 col-12 col-lg-3"]//h3[contains(text(), "Antigüedad")]').xpath('..//p/text()').extract_first()
		caracteristicas = response.xpath('//*[@class= "Li-sc-1knn373-0 ctJpwh w-90"]/p/text()').extract()
		caracteristicas = str(caracteristicas)

		self.print_('#'*90)
		self.print_('\n'+'ESTAMOS CON '+ data_link['inmu_'].upper()+ ' EN '+ data_link['type_'].upper()+ ' UBICADO EN '+ data_link['loc_'].upper()+ '\n')
		self.print_(
			f'''
			  Constructora:  {constructor}
			  Nombre proyecto:  {proyecto}
			  Estado:  {estado}
		  	  \tZona:  {zona}
		  	  \tBarrio:  {barrio}
		  	  \tBarrio comun:  {nom_barrio_comun}
		  	  \tTipo:  {tipo}
		  	  \tTitulo:  {title}
		  	  \tNegocio:  {tipo_negocio}
		  	  \t\tValor venta:  {val_venta}
		  	  \t\tValor arriedo:  {val_arriendo}
		  	  \t\t\tArea:  {area}
		  	  \t\t\t# Habitaciones:  {n_habs}
		  	  \t\t\t# Baños:  {n_banos}
		  	  \t\t\t# Garajes:  {n_garajes} \n
		  	  '''
		  	  )


		yield { 
				'mun_name':mun_name,
				'id_apto':id_apto,
				'cel_num': cel_num,
				'constructor':constructor,
				'proyecto':proyecto,
				'estado':estado,
				'zona':zona,
				'barrio':barrio,
				'nom_barrio_comun':nom_barrio_comun,
				'tipo':tipo,
				'title':title,
				'tipo_negocio':tipo_negocio,
				'estrato':estrato,
				'val_venta':val_venta,
				'val_arriendo':val_arriendo,
				'antiguedad':antiguedad,
				'area':area,
				'area_const':area_const,
				'area_priv': area_priv,
				'n_habs':n_habs,
				'n_banos':n_banos,
				'n_garajes':n_garajes,
				'caracteristicas':caracteristicas,
				'url':url
			}

		if n_apto < len(data_aptos) -1:
			n_apto += 1
			url_apto = data_aptos[n_apto]['url']

			check_connection()
			yield Request(url= url_apto,
						  callback= self.details_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								 'aptos_links':aptos_links,
								 'cache_n_from': cache_n_from,
								 'trys': trys, 
								 'data_aptos': data_aptos,
								 'n_apto': n_apto,
								 'n': n},
					  	  dont_filter= True,
					  	  headers= {'Accept': 'application/json, text/plain, */*',
								'Accept-Encoding': 'gzip, deflate, br',
								'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
								'Connection': 'keep-alive',
								'DNT': '1',
								'Host': 'www.metrocuadrado.com',
								'Upgrade-Insecure-Requests': '1',
								'Referer': cat_link,
								'Pragma': 'no-cache',
								'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0',
								'X-Api-Key': API_key,
								'X-Requested-With': 'XMLHttpRequest'})

		else:

			check_connection()
			yield Request(url= cat_link,
						  callback= self.partial_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								 'aptos_links':aptos_links,
								 'cache_n_from': cache_n_from,
								 'trys': trys, 
								 'n': n},
						  dont_filter= True)


#logging.shutdown()