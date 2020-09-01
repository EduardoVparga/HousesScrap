from scrapy import Spider
from scrapy.http import Request

from json import loads

#from time import sleep

N_categoria = 0 # Como son varias categorias construidas bajo la misma clase, se optó por acceder a cada una de ellas de manera separada para hacer más simple el codigo (hasta el dia 28/08/2020 son 4 categorias)

API_key = 'P1MfFHfQMOtL16Zpg36NcntJYCLFm8FqFfudnavl' # Toca estar pendiente si es dinamica cada cierto tiempo, por eso se deja apartarda para facilitar su manejo.






class MetrocScrapingSpider(Spider):
	name = 'metroc_scraping'
	allowed_domains = ['metrocuadrado.com']
	start_urls = ['https://www.metrocuadrado.com/']


	def parse(self, response):
		print()
		print('Entra aca 1')
		print()
		
		aptos_links = response.xpath('//*[@class= "box-list"]')[N_categoria].xpath('.//li//a/@href').extract()
		aptos_links = [link.replace('http', 'https') for link in aptos_links]

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

		print()
		print(response.url)
		print()
		print(response.status, type(response.status), 500 == response.status)
		print()



		jsonresponse = loads(response.text)
		try: test = jsonresponse['results']
		except: test = []
		#print(jsonresponse)
		print()
		print('CATEGORIA N', n_cat+1, '-------------------> count')
		print()

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

			for apto in aptos:

				mun_name = apto['mciudad']['nombre']


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

				print('#'*90)
				print('\n','ESTAMOS CON', data_link['inmu_'].upper(), 'EN', data_link['type_'].upper(), 'UBICADO EN', data_link['loc_'].upper(), '\n')
				print('\nConstructora: ', constructor,
					  '\nNombre proyecto: ', proyecto,
					  '\nEstado: ', estado,
				  	  '\n\tZona: ', zona,
				  	  '\n\tBarrio: ', barrio,
				  	  '\n\tBarrio comun: ', nom_barrio_comun,
				  	  '\n\tTipo: ', tipo,
				  	  '\n\tTitulo: ', title,
				  	  '\n\tNegocio: ', tipo_negocio,
				  	  '\n\t\tValor venta: ', val_venta,
				  	  '\n\t\tValor arriedo: ', val_arriendo,
				  	  '\n\t\t\tArea: ', area,
				  	  '\n\t\t\t# Habitaciones: ', n_habs,
				  	  '\n\t\t\t# Baños: ', n_banos,
				  	  '\n\t\t\t# Garajes: ', n_garajes, '\n')


				yield { 'mun_name': mun_name,
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
						'n_garajes': n_garajes
					}


		
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

		elif n_cat < len(aptos_links)- 1:
			n_cat += 1
			yield Request(url= 'https://www.metrocuadrado.com/',
						  callback= self.first_parse,
						  meta= {'data_links': data_links,
								 'n_cat': n_cat,
								 'aptos_links': aptos_links},
						  dont_filter= True)

