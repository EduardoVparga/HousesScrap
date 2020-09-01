from scrapy import Spider
from scrapy.http import Request

from json import loads

from time import sleep

N_categoria = 2 # Como son varias categorias construidas bajo la misma clase, se optó por acceder a cada una de ellas de manera separada para hacer más simple el codigo (hasta el dia 28/08/2020 son 4 categorias)

API_key = 'P1MfFHfQMOtL16Zpg36NcntJYCLFm8FqFfudnavl' # Toca estar pendiente si es dinamica cada cierto tiempo, por eso se deja apartarda para facilitar su manejo.


data_links = []
cat_links = []

class MetrocScrapingNproyectosSpider(Spider):
	name = 'metroc_scraping_nproyectos'
	allowed_domains = ['metrocuadrado.com']
	start_urls = ['https://www.metrocuadrado.com/']

	def parse(self, response):
		# print()
		# print('Entra aca 1')
		# print()
		
		projects_links = response.xpath('//*[@class= "box-list"]')[N_categoria].xpath('.//li//a/@href').extract()
		projects_links = [link.replace('http', 'https') for link in projects_links]

		# data_links = []

		# for url in projects_links:
		# 	items = {}
		# 	url = url.split('.com')[-1].split('/')

		# 	for ind, info in enumerate(url):
		# 		if info == '':
		# 			url.pop(ind)

		# 	items['inmu_'] = url[0]
		# 	items['type_'] = url[1]
		# 	items['loc_'] = url[-1]

		# 	data_links.append(items)

		# n_cat = 0 ################################################################################# ojo !!


		for link in projects_links[1:]:
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
	# 	n_cat = response.meta['n_cat']
			print('Entra el If de que ya se tienen todas las categorias ---------------------->')
			sleep(20)
			n_from = 0     
			n_cat = 0 			

	# 	cat_link = projects_links[n_cat]
	# 	data_link = data_links[n_cat]

	# 	print(data_link)

	# 	inmu_ = data_link['inmu_']
	# 	type_ = data_link['type_']
	# 	loc_ = data_link['loc_']

			  

	# 	n = 0

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
		
		n = response.meta['n']

	# 	inmu_ = data_link['inmu_'].replace('s', '')
	# 	type_ = data_link['type_']
	# 	loc_ = data_link['loc_']

		api_link = 'https://www.metrocuadrado.com/rest-search/search?realEstateBusinessList='+data_link['neg_']+'&realEstateStatusList='+data_link['est_']+'&city='+data_link['loc_']+'&from='+str(n_from)+'&size=300'  
		

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
							 
					  url= api_link+str(n_from)+'&size=300',
					  dont_filter= True)





	def main_parse(self, response):
	# 	data_links = response.meta['data_links']
		n_cat = response.meta['n_cat']
		n_from = response.meta['n_from']
		data_link = response.meta['data_link']
		cat_link = response.meta['cat_link']
	# 	projects_links = response.meta['projects_links']

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
		if jsonresponse['results'] != [] and n < 1:

			n_from += 300
			n += 1

			casas = jsonresponse['results']

			for casa in casas:

				mun_name = casa['mciudad']['nombre']


				constructor = casa['mnombreconstructor']
				proyecto = casa['mnombreproyecto']
				estado = casa['mestadoinmueble']


				try: zona = casa['mzona']['nombre']
				except: zona = casa['mzona']

				barrio = casa['mbarrio']
				nom_barrio_comun = casa['mnombrecomunbarrio']


				tipo = casa['mtipoinmueble']['nombre']
				title= casa['title']
				tipo_negocio = casa['mtiponegocio']
				val_venta = casa['mvalorventa']
				val_arriendo = casa['mvalorarriendo']


				area = casa['marea']
				n_habs = casa['mnrocuartos']
				n_banos = casa['mnrobanos']
				n_garajes = casa['mnrogarajes']


				image_url = casa['imageLink']

				print('#'*90)
				print('\n','ESTAMOS CON NUEVOS PROYECTOS EN', data_link['neg_'].upper(), 'UBICADO EN', data_link['loc_'].upper(), '\n')
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


				yield {
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
						  meta= {'n_cat': n_cat,
								 'n_from': n_from,
								 'data_link': data_link,
								 'cat_link': cat_link,
								 'n': n},
						  dont_filter= True)

		elif n_cat < len(data_links)- 1:
			n_cat += 1
			yield Request(url= 'https://www.metrocuadrado.com/',
						  callback= self.second_parse,
						  meta= {'n_cat': n_cat},
						  dont_filter= True)
