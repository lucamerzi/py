import pandas as pd

unique_brands = [ 
  'Acura',
  'Alfa Romeo',
  'Aston Martin',
  'Audi',
  'Bentley',
  'BMW',
  'Bugatti',
  'Buick',
  'Cadillac',
  'Chevrolet',
  'Chrysler',
  'Dodge',
  'Ferrari',
  'Fiat',
  'Ford',
  'GMC',
  'Honda',
  'Hyundai',
  'Infiniti',
  'Jaguar',
  'Jeep',
  'jeep',
  'Kia',
  'Lamborghini',
  'Land',
  'Lexus',
  'Lincoln',
  'Lotus',
  'Maserati',
  'Mazda',
  'Mercedes Benz',
  'Mini',
  'Mitsubishi',
  'Nissan',
  'Porsche',
  'Rolls Royce',
  'Subaru',
  'Tesla',
  'Toyota',
  'Volkswagen',
  'Volvo' 
]


cols_bq = ["a_Rang","a_uuid","a_afficheW","a_owner","a_country","a_latitude","a_longitude","a_ip_hashed","a_profile_date","a_insert_time","a_versionTimestamp","a_c_w0044webo_Acura_CL","a_c_w0044webo_Acura_ILX","a_c_w0044webo_Acura_Legend","a_c_w0044webo_Acura_MDX","a_c_w0044webo_Acura_NSX","a_c_w0044webo_Acura_RDX","a_c_w0044webo_Acura_RLX","a_c_w0044webo_Acura_TLX","a_c_w0044webo_Acura_TSX","a_c_w0044webo_Acura_ZDX","a_c_w0044webo_Alfa_Romeo_164","a_c_w0044webo_Alfa_Romeo_Giulia","a_c_w0044webo_Alfa_Romeo_Spider","a_c_w0044webo_Aston_Martin_DB7","a_c_w0044webo_Aston_Martin_DB9","a_c_w0044webo_Aston_Martin_DBS","a_c_w0044webo_Aston_Martin_V12_Vantage","a_c_w0044webo_Aston_Martin_V8_Vantage","a_c_w0044webo_Aston_Martin_Vanquish","a_c_w0044webo_Audi_A3","a_c_w0044webo_Audi_A3_e_tron","a_c_w0044webo_Audi_A4","a_c_w0044webo_Audi_A5","a_c_w0044webo_audi_A6","a_c_w0044webo_Audi_A7","a_c_w0044webo_Audi_A8","a_c_w0044webo_Audi_Q3","a_c_w0044webo_Audi_Q5","a_c_w0044webo_Audi_Q7","a_c_w0044webo_Audi_R8","a_c_w0044webo_Audi_TT","a_c_w0044webo_Bentley_Bentayga","a_c_w0044webo_Bentley_Continental_GT","a_c_w0044webo_Bentley_Mulsanne","a_c_w0044webo_BMW_1_Series","a_c_w0044webo_BMW_2_Series","a_c_w0044webo_BMW_3_Series","a_c_w0044webo_BMW_4_Series","a_c_w0044webo_BMW_5_Series","a_c_w0044webo_BMW_6_Series","a_c_w0044webo_BMW_7_Series","a_c_w0044webo_BMW_i3","a_c_w0044webo_BMW_i8","a_c_w0044webo_BMW_X1","a_c_w0044webo_BMW_X3","a_c_w0044webo_BMW_X5","a_c_w0044webo_BMW_X6","a_c_w0044webo_BMW_Z3","a_c_w0044webo_BMW_Z8","a_c_w0044webo_Bugatti_Chiron","a_c_w0044webo_Bugatti_Veyron","a_c_w0044webo_Buick_Enclave","a_c_w0044webo_Buick_LaCrosse","a_c_w0044webo_Buick_Regal","a_c_w0044webo_Cadillac_Allante","a_c_w0044webo_Cadillac_ATS","a_c_w0044webo_Cadillac_Catera","a_c_w0044webo_Cadillac_CT6","a_c_w0044webo_Cadillac_CTs","a_c_w0044webo_Cadillac_DTS","a_c_w0044webo_Cadillac_Eldorado","a_c_w0044webo_Cadillac_ELR","a_c_w0044webo_Cadillac_Escalade","a_c_w0044webo_Cadillac_SRX","a_c_w0044webo_Cadillac_XLR","a_c_w0044webo_Cadillac_XTS","a_c_w0044webo_Chevrolet_Astro","a_c_w0044webo_Chevrolet_Avalanche","a_c_w0044webo_Chevrolet_Bolt","a_c_w0044webo_Chevrolet_Camaro","a_c_w0044webo_Chevrolet_Cavalier","a_c_w0044webo_Chevrolet_Cobalt","a_c_w0044webo_Chevrolet_Colorado","a_c_w0044webo_Chevrolet_Corvette","a_c_w0044webo_Chevrolet_Cruze","a_c_w0044webo_Chevrolet_El_Camino","a_c_w0044webo_Chevrolet_Equinox","a_c_w0044webo_Chevrolet_Express","a_c_w0044webo_Chevrolet_HHR","a_c_w0044webo_Chevrolet_Impala","a_c_w0044webo_Chevrolet_Lumina","a_c_w0044webo_Chevrolet_Malib","a_c_w0044webo_Chevrolet_Monte_Carlo","a_c_w0044webo_Chevrolet_Silverado","a_c_w0044webo_Chevrolet_Sonic","a_c_w0044webo_Chevrolet_Spark","a_c_w0044webo_Chevrolet_Suburban","a_c_w0044webo_Chevrolet_Tahoe","a_c_w0044webo_Chevrolet_Tracker","a_c_w0044webo_Chevrolet_TrailBlazer","a_c_w0044webo_Chevrolet_Traverse","a_c_w0044webo_Chevrolet_Trax","a_c_w0044webo_Chevrolet_Uplander","a_c_w0044webo_Chevrolet_Venture","a_c_w0044webo_Chrysler_300","a_c_w0044webo_Chrysler_Concorde","a_c_w0044webo_Chrysler_Crossfire","a_c_w0044webo_Chrysler_Pacifica","a_c_w0044webo_Chrysler_PT_Cruiser","a_c_w0044webo_Chrysler_Sebring","a_c_w0044webo_Chrysler_Town_and_Country","a_c_w0044webo_Chrysler_Voyager","a_c_w0044webo_Dodge_Caliber","a_c_w0044webo_Dodge_Challenger","a_c_w0044webo_Dodge_Charger","a_c_w0044webo_Dodge_Dakota","a_c_w0044webo_Dodge_Durango","a_c_w0044webo_Dodge_Journey","a_c_w0044webo_Dodge_Magnum","a_c_w0044webo_Dodge_Viper","a_c_w0044webo_Ferrari_California","a_c_w0044webo_Ferrari_FF","a_c_w0044webo_Fiat_500","a_c_w0044webo_Fiat_500L","a_c_w0044webo_Fiat_500X","a_c_w0044webo_Ford_C_Max","a_c_w0044webo_Ford_Crown_Victoria","a_c_w0044webo_Ford_EcoSport","a_c_w0044webo_Ford_Edge","a_c_w0044webo_Ford_Escape","a_c_w0044webo_Ford_Escort","a_c_w0044webo_Ford_Excursion","a_c_w0044webo_Ford_Expedition","a_c_w0044webo_Ford_F_Series","a_c_w0044webo_ford_fiesta","a_c_w0044webo_Ford_Five_Hundred","a_c_w0044webo_Ford_Flex","a_c_w0044webo_Ford_Focus","a_c_w0044webo_Ford_Freestar","a_c_w0044webo_Ford_Fusion","a_c_w0044webo_Ford_GT","a_c_w0044webo_Ford_Mustang","a_c_w0044webo_Ford_Probe","a_c_w0044webo_Ford_Ranger","a_c_w0044webo_Ford_Taurus","a_c_w0044webo_Ford_Thunderbird","a_c_w0044webo_Ford_Transit","a_c_w0044webo_Ford_Transit_Connect","a_c_w0044webo_Ford_Windstar","a_c_w0044webo_GMC_Acadia","a_c_w0044webo_GMC_Canyon","a_c_w0044webo_GMC_Envoy","a_c_w0044webo_GMC_Sierra","a_c_w0044webo_GMC_Sonoma","a_c_w0044webo_GMC_Terrain","a_c_w0044webo_GMC_Yukon","a_c_w0044webo_Honda_Accord","a_c_w0044webo_Honda_Civic","a_c_w0044webo_Honda_CR_V","a_c_w0044webo_Honda_CR_X","a_c_w0044webo_Honda_CR_Z","a_c_w0044webo_Honda_Element","a_c_w0044webo_Honda_Fit","a_c_w0044webo_Honda_HR_V","a_c_w0044webo_Honda_Insight","a_c_w0044webo_Honda_Odyssey","a_c_w0044webo_Honda_Passport","a_c_w0044webo_Honda_Pilot","a_c_w0044webo_Honda_Ridgeline","a_c_w0044webo_Honda_S2000","a_c_w0044webo_Honda_Vigor","a_c_w0044webo_Hyundai_Accent","a_c_w0044webo_Hyundai_Elantra","a_c_w0044webo_Hyundai_Equus","a_c_w0044webo_Hyundai_Excel","a_c_w0044webo_Hyundai_Genesis","a_c_w0044webo_Hyundai_Ioniq","a_c_w0044webo_Hyundai_Santa_Fe","a_c_w0044webo_Hyundai_Sonata","a_c_w0044webo_Hyundai_Tiburon","a_c_w0044webo_Hyundai_Tucson","a_c_w0044webo_Hyundai_Veloster","a_c_w0044webo_Infiniti_Q50","a_c_w0044webo_Jaguar_F_Type","a_c_w0044webo_Jaguar_S_Type","a_c_w0044webo_Jaguar_X_Type","a_c_w0044webo_Jaguar_XE","a_c_w0044webo_Jaguar_XF","a_c_w0044webo_Jaguar_XJ","a_c_w0044webo_Jaguar_XJ_S","a_c_w0044webo_Jaguar_XK","a_c_w0044webo_Jeep_Cherokee","a_c_w0044webo_Jeep_Commander","a_c_w0044webo_Jeep_Compass","a_c_w0044webo_jeep_grand_cherokee","a_c_w0044webo_jeep_liberty","a_c_w0044webo_Jeep_Patriot","a_c_w0044webo_jeep_renegade","a_c_w0044webo_Jeep_Wrangler","a_c_w0044webo_Kia_Forte","a_c_w0044webo_Kia_Optima","a_c_w0044webo_Kia_Rio","a_c_w0044webo_Kia_Sorento","a_c_w0044webo_Kia_Soul","a_c_w0044webo_Kia_Spectra","a_c_w0044webo_Kia_Sportage","a_c_w0044webo_Lamborghini_Aventador","a_c_w0044webo_Land_Rover_Defender","a_c_w0044webo_Land_Rover_Discovery","a_c_w0044webo_Land_Rover_Discovery_Sport","a_c_w0044webo_Lexus_CT","a_c_w0044webo_Lexus_ES","a_c_w0044webo_Lexus_GS","a_c_w0044webo_Lexus_GX","a_c_w0044webo_Lexus_HS","a_c_w0044webo_Lexus_IS","a_c_w0044webo_Lexus_LC","a_c_w0044webo_Lexus_LFA","a_c_w0044webo_Lexus_LS","a_c_w0044webo_Lexus_LX","a_c_w0044webo_Lexus_NX","a_c_w0044webo_Lexus_RC","a_c_w0044webo_Lexus_RX","a_c_w0044webo_Lexus_SC","a_c_w0044webo_Lincoln_Aviator","a_c_w0044webo_Lincoln_LS","a_c_w0044webo_Lincoln_Mark_LT","a_c_w0044webo_Lincoln_MKS","a_c_w0044webo_Lincoln_MKT","a_c_w0044webo_Lincoln_MKX","a_c_w0044webo_Lincoln_MKZ","a_c_w0044webo_Lincoln_Navigator","a_c_w0044webo_Lincoln_Town_Car","a_c_w0044webo_Lotus_Elise","a_c_w0044webo_Lotus_Evora","a_c_w0044webo_Lotus_Exige","a_c_w0044webo_Maserati_Ghibli","a_c_w0044webo_Maserati_GranTurismo","a_c_w0044webo_Maserati_Quattroporte","a_c_w0044webo_mazda","a_c_w0044webo_Mazda_CX_5","a_c_w0044webo_Mazda_CX_9","a_c_w0044webo_Mazda_MPV","a_c_w0044webo_Mazda_RX_7","a_c_w0044webo_Mazda_RX_8","a_c_w0044webo_Mazda_Tribute","a_c_w0044webo_Mazda2","a_c_w0044webo_Mazda3","a_c_w0044webo_mazda5","a_c_w0044webo_Mazda6","a_c_w0044webo_Mercedes_Benz_B_Class_Electric_Drive","a_c_w0044webo_Mercedes_Benz_C_Class","a_c_w0044webo_Mercedes_Benz_E_Class","a_c_w0044webo_Mercedes_Benz_G_Class","a_c_w0044webo_Mercedes_Benz_GL_Class","a_c_w0044webo_Mercedes_Benz_M_Class","a_c_w0044webo_Mercedes_Benz_S_Class","a_c_w0044webo_Mercedes_Benz_SLR_McLaren","a_c_w0044webo_Mercedes_Benz_SLS_AMG","a_c_w0044webo_Mercedes_Benz_Sprinter","a_c_w0044webo_Mini_Clubman","a_c_w0044webo_Mini_Countryman","a_c_w0044webo_mini_cooper","a_c_w0044webo_Mitsubishi_Diamante","a_c_w0044webo_Mitsubishi_Galant","a_c_w0044webo_Mitsubishi_i_MiEV","a_c_w0044webo_Mitsubishi_Lancer","a_c_w0044webo_Mitsubishi_Mirage","a_c_w0044webo_Mitsubishi_Outlander","a_c_w0044webo_Mitsubishi_Pajero","a_c_w0044webo_Nissan_300ZX","a_c_w0044webo_Nissan_350Z","a_c_w0044webo_Nissan_370Z","a_c_w0044webo_Nissan_Altima","a_c_w0044webo_Nissan_Armada","a_c_w0044webo_Nissan_Cube","a_c_w0044webo_Nissan_Frontier","a_c_w0044webo_Nissan_GT_R","a_c_w0044webo_Nissan_Juke","a_c_w0044webo_Nissan_Leaf","a_c_w0044webo_Nissan_Maxima","a_c_w0044webo_Nissan_Murano","a_c_w0044webo_Nissan_NV200","a_c_w0044webo_Nissan_Pathfinder","a_c_w0044webo_Nissan_Quest","a_c_w0044webo_Nissan_Rogue","a_c_w0044webo_Nissan_Sentra","a_c_w0044webo_Nissan_Titan","a_c_w0044webo_Nissan_Xterra","a_c_w0044webo_Porsche_911","a_c_w0044webo_Porsche_928","a_c_w0044webo_Porsche_944","a_c_w0044webo_Porsche_968","a_c_w0044webo_Porsche_Carrera_GT","a_c_w0044webo_Porsche_Cayenne","a_c_w0044webo_Porsche_Cayman","a_c_w0044webo_Porsche_Panamera","a_c_w0044webo_Rolls_Royce_Ghost","a_c_w0044webo_Rolls_Royce_Phantom","a_c_w0044webo_Subaru_BRAT","a_c_w0044webo_Subaru_BRZ","a_c_w0044webo_Subaru_Forester","a_c_w0044webo_Subaru_Impreza","a_c_w0044webo_Subaru_Legacy","a_c_w0044webo_Subaru_Leone","a_c_w0044webo_Subaru_Outback","a_c_w0044webo_Subaru_Tribeca","a_c_w0044webo_Subaru_WRX","a_c_w0044webo_Subaru_XT","a_c_w0044webo_Tesla_Model_3","a_c_w0044webo_Tesla_Model_S","a_c_w0044webo_Tesla_Model_X","a_c_w0044webo_Toyota_4Runner","a_c_w0044webo_Toyota_Avalon","a_c_w0044webo_Toyota_Camry","a_c_w0044webo_Toyota_Celica","a_c_w0044webo_Toyota_Corolla","a_c_w0044webo_Toyota_Highlander","a_c_w0044webo_Toyota_Land_Cruiser","a_c_w0044webo_Toyota_Mirai","a_c_w0044webo_Toyota_Prius","a_c_w0044webo_Toyota_RAV4","a_c_w0044webo_Toyota_Sequoia","a_c_w0044webo_Toyota_Sienna","a_c_w0044webo_Toyota_Supra","a_c_w0044webo_Toyota_Tacoma","a_c_w0044webo_Toyota_Tercel","a_c_w0044webo_Toyota_Tundra","a_c_w0044webo_Toyota_Vitz","a_c_w0044webo_Toyota_Yaris","a_c_w0044webo_Volkswagen_Beetle","a_c_w0044webo_Volkswagen_e_Golf","a_c_w0044webo_Volkswagen_Eos","a_c_w0044webo_Volkswagen_Fox","a_c_w0044webo_Volkswagen_Golf","a_c_w0044webo_Volkswagen_Golf_GTI","a_c_w0044webo_Volkswagen_Jetta","a_c_w0044webo_volkswagen_passat","a_c_w0044webo_Volkswagen_Phaeton","a_c_w0044webo_Volkswagen_Routan","a_c_w0044webo_Volkswagen_Scirocco","a_c_w0044webo_Volkswagen_Tiguan","a_c_w0044webo_Volkswagen_Transporter","a_c_w0044webo_Volvo_C30","a_c_w0044webo_Volvo_C70","a_c_w0044webo_Volvo_S40","a_c_w0044webo_Volvo_S60","a_c_w0044webo_Volvo_S80","a_c_w0044webo_Volvo_V60","a_c_w0044webo_Volvo_V70","a_c_w0044webo_Volvo_XC60","a_c_w0044webo_Volvo_XC70","a_c_w0044webo_Volvo_XC90"]


def write_query_brand(brandname, startStr, endStr):

	def select_bq_cols(brandname):

		if " " in brandname:
			brandname = brandname.replace(" ", "_")

		return [a for a in cols_bq if brandname in a]

	def writecasewhen(cols):

		return "\n".join(["WHEN {0} BETWEEN 1 AND 5 THEN 'Interested'\n WHEN {0} BETWEEN 6 AND 10 THEN 'Higly Interested'\n WHEN {0} BETWEEN 11 AND 15 THEN 'Engaged'\n WHEN {0} BETWEEN 16 AND 20 THEN 'Intenders'\n  ".format(a) for a in cols])

	def writecarmodel(arr):
		# return "\n".join([a+"," for a in arr])
		return "\n".join(["{},".format(a) for a in arr])

	def writecarmodelisnotnull(arr):
	  # return "\n".join([a+"," for a in arr])
	  return ("\n".join(["{} IS NOT NULL OR".format(a) for a in arr]))[:-2]




	relevantcols=select_bq_cols(brandname)

	query = """ (
	SELECT
	'{0}' AS Brand,
	NULL AS Model,
	/*-------------------*/
	CASE
			{1}
	/*-------------------*/
	END AS Intensity_Level,
	COUNT(UNIQUE(a_afficheW)) AS UU
	FROM (
	SELECT
	a_afficheW,
	{2}




	ROW_NUMBER() OVER (PARTITION BY a_afficheW ORDER BY versionTimestamp DESC) Rg
	FROM (
	SELECT
		a_afficheW,
		{2}
		INTEGER(a_versionTimestamp) AS versionTimestamp
	FROM
		table_date_range([datamining-1184:AutomotiveUS.automotiveUS_uu_],
		TIMESTAMP('{3}'),
		TIMESTAMP('{4}'))
	WHERE
  {5}
    ))
	WHERE
	Rg=1
	GROUP BY
	Brand,
	Model,
	Intensity_Level
  )

			""".format(brandname, writecasewhen(relevantcols), writecarmodel(relevantcols), startStr, endStr, writecarmodelisnotnull(relevantcols))



	return query

def write_query_full(brandarr,startStr, endStr):

  query = """
          SELECT *
          FROM {}
          """.format(",".join ( [ write_query_brand(b, startStr, endStr) for b in brandarr ] ) )

  return query

print(write_query_full(unique_brands, '20181015', '20181115' ))