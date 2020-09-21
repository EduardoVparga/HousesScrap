from scrapy import Spider
from scrapy.http import Request

from json import loads

from time import sleep

import os, logging

from datetime import date


N_categoria = -1 # Como son varias categorias construidas bajo la misma clase, se optó por acceder a cada una de ellas de manera separada para hacer más simple el codigo (hasta el dia 28/08/2020 son 4 categorias)

API_key = 'P1MfFHfQMOtL16Zpg36NcntJYCLFm8FqFfudnavl' # Toca estar pendiente si es dinamica cada cierto tiempo, por eso se deja apartarda para facilitar su manejo.



class MetrocScrapingOtroSpider(Spider):
	name = 'metroc_scraping_otro'
	allowed_domains = ['metrocuadrado.com']
	start_urls = ['http://metrocuadrado.com/']

	

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

		print()
		print('Entra aca 1')
		print()
		
		otros_links = response.xpath('//*[@class= "box-list"]')[N_categoria].xpath('.//li//a/@href').extract()
		otros_links = [link.replace('http', 'https') for link in otros_links]

		data_links = []

		for url in otros_links:
			items = {}
			url = url.split('.com')[-1].split('/')

			for ind, info in enumerate(url):
				if info == '':
					url.pop(ind)

			items['inmu_'] = url[0]
			items['type_'] = url[-1]

			data_links.append(items)

		for n in otros_links:
			print(n)

		print()
		print()

		for x in data_links:
			print(x)		


		n_cat = 0 ################################################################################# ojo !!

		yield Request(url= response.url,
					  callback= self.first_parse,
					  meta= {'data_links': data_links,
							 'n_cat': n_cat,
							 'otros_links': otros_links},
					  dont_filter= True)


	def first_parse(self, response):
		data_links = response.meta['data_links']
		n_cat = response.meta['n_cat']
		otros_links = response.meta['otros_links']
		n_from = 0 ###################################################################### ojo     			

		cat_link = otros_links[n_cat]
		data_link = data_links[n_cat]

		print(data_link)

		inmu_ = data_link['inmu_']
		type_ = data_link['type_']

		api_link = 'https://www.metrocuadrado.com/rest-search/search?seo=/'+inmu_+'/'+type_+'/&from='+str(n_from)+'&size=300'  

		n = 0
		trys = 0
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
							 'otros_links':otros_links,
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
		otros_links = response.meta['otros_links']
		trys = response.meta['trys']

		cache_n_from = response.meta['cache_n_from']

		n = response.meta['n']

		inmu_ = data_link['inmu_'].replace('s', '')
		type_ = data_link['type_']
		#loc_ = data_link['loc_']

		api_link = 'https://www.metrocuadrado.com/rest-search/search?realEstateTypeList='+inmu_+'&realEstateBusinessList='+type_+'&from='  
		

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
							 'otros_links':otros_links,
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
		otros_links = response.meta['otros_links']

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
			yield Request(url= cat_link,
						  callback= self.partial_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								 'otros_links':otros_links,
								 'cache_n_from': cache_n_from,
								 'trys': trys,
								 'n': n},
						  dont_filter= True)



		elif test != [] and response.status not in [500]: # and n < 1:

			cache_n_from = n_from
			trys = 0

			n_from += 300
			n += 1

			otros = jsonresponse['results']

			url_otros = []
			data_otros = []
			n_otro = 0
			
			for otro in otros:
				#data_otro = {}

				id_otro = otro['midinmueble']

				mun_name = otro['mciudad']['nombre']

				cel_num = apto['contactPhone']

				constructor = otro['mnombreconstructor']
				proyecto = otro['mnombreproyecto']
				estado = otro['mestadoinmueble']


				try: zona = otro['mzona']['nombre']
				except: zona = otro['mzona']

				barrio = otro['mbarrio']
				nom_barrio_comun = otro['mnombrecomunbarrio']


				tipo = otro['mtipoinmueble']['nombre']
				title= otro['title']
				tipo_negocio = otro['mtiponegocio']
				val_venta = otro['mvalorventa']
				val_arriendo = otro['mvalorarriendo']


				area = otro['marea']
				n_habs = otro['mnrocuartos']
				n_banos = otro['mnrobanos']
				n_garajes = otro['mnrogarajes']


				image_url = otro['imageLink']

				url = 'https://www.metrocuadrado.com' + otro['link']
				
				data_otro =  { 
							   'id_otro': id_otro,
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
				data_otros.append(data_otro)


			url_otro = data_otros[n_otro]['url']

			print(data_otros[n_otro])

			yield Request(url= url_otro,
						  callback= self.details_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								 'otros_links':otros_links,
								 'cache_n_from': cache_n_from,
								 'trys': trys, 
								 'data_otros': data_otros,
								 'n_otro': n_otro,
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



		elif n_cat < len(otros_links)- 1:
			n_cat += 1
			yield Request(url= 'https://www.metrocuadrado.com/',
						  callback= self.first_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'otros_links': otros_links},
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
		otros_links= response.meta['otros_links']
		cache_n_from= response.meta['cache_n_from']
		trys= response.meta['trys']
		data_otros = response.meta['data_otros']
		n_otro = response.meta['n_otro']

		n= response.meta['n']	

		self.print_()
		self.print_(response.url)
		self.print_()
		#self.print_(response.text)
		self.print_()

		#self.print_(otro)

		id_otro = data_otros[n_otro]['id_otro']
		mun_name = data_otros[n_otro]['mun_name']
		cel_num = data_otros[n_otro]['cel_num']
		constructor = data_otros[n_otro]['constructor']
		proyecto = data_otros[n_otro]['proyecto']
		estado = data_otros[n_otro]['estado']
		zona = data_otros[n_otro]['zona']
		barrio = data_otros[n_otro]['barrio']
		nom_barrio_comun = data_otros[n_otro]['nom_barrio_comun']
		tipo = data_otros[n_otro]['tipo']
		title = data_otros[n_otro]['title']
		tipo_negocio = data_otros[n_otro]['tipo_negocio']
		val_venta = data_otros[n_otro]['val_venta']
		val_arriendo = data_otros[n_otro]['val_arriendo']
		area = data_otros[n_otro]['area']
		n_habs = data_otros[n_otro]['n_habs']
		n_banos = data_otros[n_otro]['n_banos']
		n_garajes = data_otros[n_otro]['n_garajes']
		url = data_otros[n_otro]['url']

		estrato = response.xpath('//h2[@class= "H2-kplljn-0 fxUScU card-text"]/text()').extract_first()
		area_const = response.xpath('//*[@class= "Col-sc-14ninbu-0 lfGZKA mb-3 pb-1 col-12 col-lg-3"]//h3[contains(text(), "Área construida")]').xpath('..//p/text()').extract_first()
		area_priv = response.xpath('//*[@class= "Col-sc-14ninbu-0 lfGZKA mb-3 pb-1 col-12 col-lg-3"]//h3[contains(text(), "Área privada")]').xpath('..//p/text()').extract_first()
		antiguedad = response.xpath('//*[@class= "Col-sc-14ninbu-0 lfGZKA mb-3 pb-1 col-12 col-lg-3"]//h3[contains(text(), "Antigüedad")]').xpath('..//p/text()').extract_first()
		caracteristicas = response.xpath('//*[@class= "Li-sc-1knn373-0 ctJpwh w-90"]/p/text()').extract()
		caracteristicas = str(caracteristicas)

		self.print_('#'*90)
		self.print_('\n'+'ESTAMOS CON '+ data_link['inmu_'].upper()+ ' EN '+ data_link['type_'].upper()+ ' UBICADO EN '+ '\n')
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
				'id_otro':id_otro,
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

		if n_otro < len(data_otros) -1:
			n_otro += 1
			url_otro = data_otros[n_otro]['url']

			yield Request(url= url_otro,
						  callback= self.details_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								 'otros_links':otros_links,
								 'cache_n_from': cache_n_from,
								 'trys': trys, 
								 'data_otros': data_otros,
								 'n_otro': n_otro,
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
			yield Request(url= cat_link,
						  callback= self.partial_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								 'otros_links':otros_links,
								 'cache_n_from': cache_n_from,
								 'trys': trys, 
								 'n': n},
						  dont_filter= True)

