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

N_categoria = 2 # Como son varias categorias construidas bajo la misma clase, se optó por acceder a cada una de ellas de manera separada para hacer más simple el codigo (hasta el dia 28/08/2020 son 4 categorias)

API_key = 'P1MfFHfQMOtL16Zpg36NcntJYCLFm8FqFfudnavl' # Toca estar pendiente si es dinamica cada cierto tiempo, por eso se deja apartarda para facilitar su manejo.


data_links = []
cat_links = []


class MetrocScrapingNproyectosSpider(Spider):
	name = 'metroc_scraping_nproyectos'
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
			LOG_FILENAME = '.\\logs\\projects_log_' + str(date.today()) + '.log'

			for handler in logging.root.handlers[:]:
			    logging.root.removeHandler(handler)
			
			logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG, format= '%(asctime)s : %(levelname)s : %(message)s')    
			logging.info('Forecastiong Job Started...')
		
		except FileNotFoundError:
			os.mkdir(os.getcwd() + '\\logs')	
			LOG_FILENAME = '.\\logs\\projects_log_' + str(date.today()) + '.log'

			for handler in logging.root.handlers[:]:
			    logging.root.removeHandler(handler)
			
			logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG, format= '%(asctime)s : %(levelname)s : %(message)s')    
			logging.info('Forecastiong Job Started...')


		
		projects_links = response.xpath('//*[@class= "box-list"]')[N_categoria].xpath('.//li//a/@href').extract()
		projects_links = [link.replace('http', 'https') for link in projects_links]


		for link in projects_links[1:]:
			check_connection()
			yield Request(url= link,
						  callback= self.first_parse,
						  meta= {'projects_links': projects_links},
						  dont_filter= True)




	def first_parse(self, response):
		projects_links = response.meta['projects_links']

		print()
		print(response.url)
		print()
		print(response.status)
		print()
		cat_url = response.xpath('//*[@class= "col-xs-12 center"]/a/@href').extract()
		cat_url = list(dict.fromkeys([i.strip() for i in cat_url]))
		cat_url = cat_url[0]
		cat_link = cat_url[0]
		print(cat_url)
		print()

		cat_url = cat_url.split('?')[0].split('/')

		data_link = {}
		for ind, info in enumerate(cat_url):
			if info == '':
				cat_url.pop(ind)

		data_link['neg_'] = cat_url[0]
		data_link['loc_'] = cat_url[1]
		data_link['est_'] = cat_url[-1]

		print(cat_url)
		print()

		data_links.append(data_link)
		cat_links.append('https://www.metrocuadrado.com'+cat_link)

		if len(data_links) == len(projects_links)-1:	
			print('Entra el If de que ya se tienen todas las categorias ---------------------->')
			sleep(20)
			n_from = 0     
			n_cat = 0 			

			check_connection()
			yield Request(url= 'https://www.metrocuadrado.com/',
						  callback= self.second_parse,
						  meta= {'n_cat': n_cat,
						  		 'n_from': n_from},
						  dont_filter= True)

			


	def second_parse(self, response):
		n_cat = response.meta['n_cat']
		n_from = 0

		n = 0

		data_link = data_links[n_cat]
		cat_link = cat_links[n_cat] 

		api_link = 'https://www.metrocuadrado.com/rest-search/search?seo=/'+data_link['neg_']+'/'+data_link['loc_']+'/'+data_link['est_']+'/&from='+str(n_from)+'&size=300'

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
					  meta= {
							 'n_from': n_from,
							 'n_cat': n_cat,
							 'data_link': data_link,
							 'cat_link': cat_link,
							 'n': n},
					  
					  url= api_link,
					  
					  dont_filter= True
					  
					  )




	def partial_parse(self, response):
		
		n_cat = response.meta['n_cat']
		n_from = response.meta['n_from']
		data_link = response.meta['data_link']
		cat_link = response.meta['cat_link']
		#projects_links = response.meta['projects_links']
		
		n = response.meta['n']

		api_link = 'https://www.metrocuadrado.com/rest-search/search?realEstateBusinessList='+data_link['neg_']+'&realEstateStatusList='+data_link['est_']+'&city='+data_link['loc_']+'&from='+str(n_from)+'&size=300'  
		

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

					  meta= {'n_cat': n_cat,
							 'n_from': n_from,
							 'data_link': data_link,
							 'cat_link': cat_link,
							 'n': n},
							 
					  url= api_link, #+str(n_from)+'&size=300',
					  dont_filter= True)





	def main_parse(self, response):
	# 	data_links = response.meta['data_links']
		n_cat = response.meta['n_cat']
		n_from = response.meta['n_from']
		data_link = response.meta['data_link']
		cat_link = response.meta['cat_link']
		#projects_links = response.meta['projects_links']

		n = response.meta['n']

		print()
		print(response.url)
		print()
		print(response.status)
		print()



		jsonresponse = loads(response.text)
		#print(jsonresponse)
		print()
		print('CATEGORIA N', n_cat+1, '-------------------> count')
		print()

	# 	#sleep(5)
		if jsonresponse['results'] != []: # and n < 1:

			n_from += 300
			n += 1

			projects = jsonresponse['results']

			url_projects = []
			data_projects = []
			n_project = 0

			for project in projects:

				id_project = project['midinmueble']

				mun_name = project['mciudad']['nombre']

				cel_num = project['contactPhone']

				constructor = project['mnombreconstructor']
				proyecto = project['mnombreproyecto']
				estado = project['mestadoinmueble']


				try: zona = project['mzona']['nombre']
				except: zona = project['mzona']

				barrio = project['mbarrio']
				nom_barrio_comun = project['mnombrecomunbarrio']


				tipo = project['mtipoinmueble']['nombre']
				title= project['title']
				tipo_negocio = project['mtiponegocio']
				val_venta = project['mvalorventa']
				val_arriendo = project['mvalorarriendo']


				area = project['marea']
				n_habs = project['mnrocuartos']
				n_banos = project['mnrobanos']
				n_garajes = project['mnrogarajes']


				image_url = project['imageLink']

				url = 'https://www.metrocuadrado.com' + project['link']



				data_project =  { 
							   'id_project': id_project,
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
				data_projects.append(data_project)

			url_project = data_projects[n_project]['url']

			check_connection()
			yield Request(url= url_project,
						  callback= self.details_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								# 'projects_links':projects_links,
								# 'cache_n_from': cache_n_from,
								# 'trys': trys, 
								 'data_projects': data_projects,
								 'n_project': n_project,
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



		elif n_cat < len(data_links)- 1:
			n_cat += 1

			check_connection()
			yield Request(url= 'https://www.metrocuadrado.com/',
						  callback= self.second_parse,
						  meta= {'n_cat': n_cat},
						  dont_filter= True)

		else: 
			logging.shutdown()
			self.print_('#'*20+' Por ahora terminamos '+'#'*20)




	def details_parse(self, response):
		data_links= response.meta['data_links']
		n_cat= response.meta['n_cat']
		n_from= response.meta['n_from']
		data_link= response.meta['data_link']
		cat_link= response.meta['cat_link']
		#projects_links= response.meta['projects_links']
		#cache_n_from= response.meta['cache_n_from']
		#trys= response.meta['trys']
		data_projects = response.meta['data_projects']
		n_project = response.meta['n_project']

		n= response.meta['n']	

		self.print_()
		self.print_(response.url)
		self.print_()
		#self.print_(response.text)
		self.print_()

		#self.print_(project)

		id_project = data_projects[n_project]['id_project']
		mun_name = data_projects[n_project]['mun_name']
		cel_num = data_projects[n_project]['cel_num']
		constructor = data_projects[n_project]['constructor']
		proyecto = data_projects[n_project]['proyecto']
		estado = data_projects[n_project]['estado']
		zona = data_projects[n_project]['zona']
		barrio = data_projects[n_project]['barrio']
		nom_barrio_comun = data_projects[n_project]['nom_barrio_comun']
		tipo = data_projects[n_project]['tipo']
		title = data_projects[n_project]['title']
		tipo_negocio = data_projects[n_project]['tipo_negocio']
		val_venta = data_projects[n_project]['val_venta']
		val_arriendo = data_projects[n_project]['val_arriendo']
		area = data_projects[n_project]['area']
		n_habs = data_projects[n_project]['n_habs']
		n_banos = data_projects[n_project]['n_banos']
		n_garajes = data_projects[n_project]['n_garajes']
		url = data_projects[n_project]['url']

		estrato = response.xpath('//h2[@class= "H2-kplljn-0 fxUScU card-text"]/text()').extract_first()
		area_const = response.xpath('//*[@class= "Col-sc-14ninbu-0 lfGZKA mb-3 pb-1 col-12 col-lg-3"]//h3[contains(text(), "Área construida")]').xpath('..//p/text()').extract_first()
		area_priv = response.xpath('//*[@class= "Col-sc-14ninbu-0 lfGZKA mb-3 pb-1 col-12 col-lg-3"]//h3[contains(text(), "Área privada")]').xpath('..//p/text()').extract_first()
		antiguedad = response.xpath('//*[@class= "Col-sc-14ninbu-0 lfGZKA mb-3 pb-1 col-12 col-lg-3"]//h3[contains(text(), "Antigüedad")]').xpath('..//p/text()').extract_first()
		caracteristicas = response.xpath('//*[@class= "Li-sc-1knn373-0 ctJpwh w-90"]/p/text()').extract()
		caracteristicas = str(caracteristicas)

		self.print_('#'*90)
		self.print_('\n'+'ESTAMOS CON NUEVOS PROYECTOS EN ' + data_link['neg_'].upper() + ' UBICADO EN ' + data_link['loc_'].upper()+ '\n')
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
				'id_project':id_project,
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

		if n_project < len(data_projects) -1:
			n_project += 1
			url_project = data_projects[n_project]['url']

			check_connection()
			yield Request(url= url_project,
						  callback= self.details_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								# 'projects_links':projects_links,
								# 'cache_n_from': cache_n_from,
								# 'trys': trys, 
								 'data_projects': data_projects,
								 'n_project': n_project,
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
								# 'projects_links':projects_links,
								# 'cache_n_from': cache_n_from,
								# 'trys': trys, 
								 'n': n},
						  dont_filter= True)