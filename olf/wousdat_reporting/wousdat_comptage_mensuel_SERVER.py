import numpy as np
import pandas as pd
import random
import time
import datetime
import os
import smtplib
import subprocess
from gcloud import datastore, bigquery, storage

# os.chdir("Desktop/")

start_date_bq = str(datetime.date.today() - datetime.timedelta(float(31))).replace('-','') 
#print("I am calculating from the date: " + start_date_bq)

end_date_bq = str(datetime.date.today() - datetime.timedelta(float(1))).replace('-','')
#print("I am calculating up to the date: " + end_date_bq)

project= 'datamining-1184'
bgclient = bigquery.Client(project=project)
stclient = storage.Client(project=project)
def query_BQ (query):
    project= 'datamining-1184'
    bgclient = bigquery.Client(project=project)
    stclient = storage.Client(project=project)
    query_results = bgclient.run_sync_query(query)
    query_results.use_legacy_sql = True
    query_results.timeout_ms=300000
    query_results.run()
    final_rows=[]
    page_token=None
    while True:
        rows, total_rows, page_token = query_results.fetch_data(max_results=10000,page_token=page_token)
        for row in rows:
            final_rows.append(row)
        if not page_token:
            break
    return pd.DataFrame(final_rows,columns=[a.name for a in query_results.schema])


    
    df=query_BQ(query_string)   
    print(len(df))
    df.head()


liste_de_pays = ['FR','IT','ES']

folder_destination ='/home/production/programmatique/'
filepath = folder_destination + "report_wousdat_" + start_date_bq + "_" + end_date_bq + ".xlsx"

writer=pd.ExcelWriter(filepath, engine='xlsxwriter')


for i in range(3):
    query_name = 'compte_' + str(liste_de_pays[i])
    a = liste_de_pays[i]
    print(a)

    count_w = """Select
    /*avg(sd__8) /(avg(sd__8)+ avg(sd__6)) as Male,
    avg(sd__6) /(avg(sd__8)+ avg(sd__6)) as Female,
    avg(sd__4) /(avg(sd__4)+ avg(sd__7)+ avg(sd__11)+ avg(sd__12)+ avg(sd__9)) as aged_18_to_24,
    avg(sd__7) /(avg(sd__4)+ avg(sd__7)+ avg(sd__11)+ avg(sd__12)+ avg(sd__9)) as aged_25_to_34,
    avg(sd__11) /(avg(sd__4)+ avg(sd__7)+ avg(sd__11)+ avg(sd__12)+ avg(sd__9)) as aged_35_to_49,
    avg(sd__12) /(avg(sd__4)+ avg(sd__7)+ avg(sd__11)+ avg(sd__12)+ avg(sd__9)) as aged_50_to_64,
    avg(sd__9) /( avg(sd__4)+ avg(sd__7)+ avg(sd__11)+ avg(sd__12)+ avg(sd__9)) as aged_65_and_more,
    avg(sd__5) / (avg(sd__5) + avg(sd__23) + avg(sd__22) + avg(sd__13)) as student,
    avg(sd__23) / (avg(sd__5) + avg(sd__23) + avg(sd__22) + avg(sd__13)) as lower_middle_class,
    avg(sd__22) / (avg(sd__5) + avg(sd__23) + avg(sd__22) + avg(sd__13)) as upper_middle_class,
    avg(sd__13) / (avg(sd__5) + avg(sd__23) + avg(sd__22) + avg(sd__13)) as retired,*/
    count(*) as Total_hits_june_2018,
    sum(c__1) as Family,
    sum(c__2) as Fast_food,
    sum(c__3) as Fantasy,
    sum(c__5) as Art,
    sum(c__7) as Computer_Science,
    sum(c__9) as Games_consoles,
    sum(c__11) as Teaching,
    sum(c__13) as Tourism,
    sum(c__15) as Nature,
    sum(c__16) as Air_conditioning,
    sum(c__17) as History,
    sum(c__19) as Auto_parts,
    sum(c__20) as Painting,
    sum(c__23) as Music,
    sum(c__25) as Asset_management,
    sum(c__27) as Jewelry,
    sum(c__29) as Astrology,
    sum(c__30) as Hair_products_and_styling,
    sum(c__31) as Horse_racing,
    sum(c__32) as Bullfighting,
    sum(c__33) as Holidays,
    sum(c__35) as Real_estate,
    sum(c__37) as Video_games,
    sum(c__39) as Interior_design,
    sum(c__40) as Hacking,
    sum(c__41) as Comedy,
    sum(c__43) as Gas_and_electricity,
    sum(c__45) as Going_out,
    sum(c__47) as Business_Administration_and_Management,
    sum(c__49) as Healthcare_and_medicine,
    sum(c__51) as Sports,
    sum(c__52) as Soccer_and_cycling,
    sum(c__53) as Fauna,
    sum(c__54) as Health,
    sum(c__55) as A_Levels,
    sum(c__56) as Psychotherapy,
    sum(c__59) as Manga,
    sum(c__60) as Relaxation_therapy,
    sum(c__61) as TV_Shows,
    sum(c__63) as Gambling,
    sum(c__65) as Software_development,
    sum(c__67) as Careers_and_occupational_training,
    sum(c__69) as Politics,
    sum(c__71) as Holiday_rentals,
    sum(c__73) as Car_buyers,
    sum(c__75) as Software,
    sum(c__77) as Films,
    sum(c__79) as Cooking,
    sum(c__81) as Graphic_design,
    sum(c__83) as Fine_dining_and_local_produce,
    sum(c__84) as Theatre,
    sum(c__85) as Telecom_operators,
    sum(c__86) as Occult,
    sum(c__87) as Kitchen_Appliances,
    sum(c__89) as Catch_up_TV,
    sum(c__90) as Cosmetic_surgery,
    sum(c__91) as Clothing,
    sum(c__92) as Savings,
    sum(c__93) as Building_and_civil_engineering,
    sum(c__95) as DIY_Equipment,
    sum(c__97) as Diet_and_nutrition,
    sum(c__99) as Makeup,
    sum(c__101) as Pregnancy,
    sum(c__102) as Philosophy,
    sum(c__103) as Computer_hardware_and_devices,
    sum(c__105) as Major_Appliances_White_goods,
    sum(c__107) as Labour_law,
    sum(c__108) as Meat_and_Fish,
    sum(c__109) as Law,
    sum(c__110) as Fruits_and_vegetables,
    sum(c__111) as Finance,
    sum(c__112) as Geometry,
    sum(c__113) as Public_administrations,
    sum(c__115) as Banking,
    sum(c__116) as Motor_Sport,
    sum(c__117) as Higher_education,
    sum(c__119) as Soccer,
    sum(c__120) as ISP_and_Browsers,
    sum(c__124) as Spanish_Travel,
    sum(c__125) as Fashion_trend,
    sum(c__129) as Ecology,
    sum(c__131) as Supermarkets,
    sum(c__133) as Infants_and_children,
    sum(c__134) as Hiking_and_Mountaineering,
    sum(c__135) as Dating,
    sum(c__137) as New_Year,
    sum(c__138) as Astronomy,
    sum(c__139) as Back_to_school,
    sum(c__140) as Consumer_Electronics_Brown_goods,
    sum(c__147) as Good_deals,
    sum(c__149) as Travel_in_France,
    sum(c__151) as Accessories,
    sum(c__153) as Outdoor_activities,
    sum(c__154) as Event_Planning,
    sum(c__155) as News,
    sum(c__157) as Farmer,
    sum(c__158) as Children,
    sum(c__159) as Alcohol,
    sum(c__161) as Furniture,
    sum(c__162) as Swimming,
    sum(c__163) as Arts_and_crafts,
    sum(c__164) as Dentist,
    sum(c__165) as Insurance,
    sum(c__167) as Cars,
    sum(c__168) as Anatomy,
    sum(c__169) as Beauty_products,
    sum(c__170) as Science,
    sum(c__171) as Soft_drinks,
    sum(c__173) as DIY,
    sum(c__174) as Language,
    sum(c__175) as Beauty_treatments,
    sum(c__177) as Tv_channels,
    sum(c__179) as Hunting_and_Fishing,
    sum(c__180) as South_American_Travel,
    sum(c__181) as Footwear,
    sum(c__183) as Pets,
    sum(c__185) as Comics,
    sum(c__187) as Savoury_food,
    sum(c__188) as Dance,
    sum(c__189) as Desserts,
    sum(c__190) as Monastery_and_convent,
    sum(c__191) as Culture_purchases,
    sum(c__193) as Sports_equipments_and_Outdoor_gear,
    sum(c__195) as Horse_riding,
    sum(c__197) as Gardening,
    sum(c__199) as Toys_and_games,
    sum(c__200) as Medicine,
    sum(c__201) as Lingerie,
    sum(c__202) as Martial_arts,
    sum(c__203) as Literature,
    sum(c__204) as Cruises,
    sum(c__205) as Eyewear,
    sum(c__207) as Car_brands,
    sum(c__208) as Pharmacy,
    sum(c__209) as Motorcycles_and_bicycles,
    sum(c__210) as Bodybuilding,
    sum(c__211) as Classical_music_and_instruments,
    sum(c__212) as Weather,
    sum(c__213) as Fragrance,
    sum(c__215) as Hair_Products,
    sum(c__216) as Golf,
    sum(c__217) as Health_and_Care_Products,
    sum(c__219) as Personal_care,
    sum(c__220) as Advertising,
    sum(c__222) as Weapons,
    sum(c__223) as Cinemas,
    sum(c__225) as Stop_smoking,
    sum(c__227) as Christmas,
    sum(c__229) as High_Fashion,
    sum(c__231) as Marriage___civil_union,
    sum(c__233) as Cameras,
    sum(c__235) as Social_networks,
    sum(c__237) as Tennis,
    sum(c__239) as Cycling,
    sum(c__241) as Winter_Holidays,
    sum(c__243) as Rugby,
    sum(c__4) as Architect,
    sum(c__6) as Tradesman,
    sum(c__8) as Lawyer,
    sum(c__10) as Doctor,
    sum(c__12) as Merchant,
    sum(c__14) as Entrepreneur,
    sum(c__21) as Cloud_computing,
    sum(c__22) as Energy,
    sum(c__24) as Laundry,
    sum(c__26) as Tablet,
    sum(c__28) as Cats,
    sum(c__18) as Basketball,
    sum(c__34) as Bicycle,
    sum(c__36) as Running,
    sum(c__42) as Amusement_park,
    sum(c__44) as Loans,
    sum(c__46) as Deodorant,
    sum(c__38) as Laptop,
    sum(c__62) as Medical_brand,
    sum(c__50) as Car_Rental,
    sum(c__48) as Moving_House,
    sum(c__142) as Alps_Killy,
    sum(c__70) as Halloween,
    sum(c__144) as Alps_3_Valleys,
    sum(c__58) as Baseball,
    sum(c__74) as Luxury,
    sum(c__57) as American_Football,
    sum(c__66) as Utility_vehicles,
    sum(c__68) as Insurance_churning,
    sum(c__64) as Local_car_brands,
    sum(c__141) as Churn_Telecom,
    sum(c__80) as Medical_Schools,
    sum(c__78) as Top_French_Engineering_schools,
    sum(c__76) as Top_Business_schools,
    sum(c__88) as Dependence,
    sum(c__94) as Retirement_period,
    sum(c__96) as Heritage_transmission,
    sum(c__98) as Auto_C___Crossover,
    sum(c__100) as Auto_C___Compact_cars,
    sum(c__104) as Auto_B___Compact_Crossover,
    sum(c__106) as Auto_A___Urban_cars,
    sum(c__114) as Auto_D___Sedan,
    sum(c__118) as Auto_E___Minivan,
    sum(c__121) as Auto_B___Mid_size_urban_cars,
    sum(c__122) as Auto_Elec___Electrical_car,
    sum(c__123) as Auto_Brand_A,
    sum(c__130) as Sunny_Destination,
    sum(c__132) as Air_Transport,
    sum(c__136) as Rail_Transport,
    sum(c__146) as Gluten_Free,
    sum(c__148) as Organic_and_Km0_food,
    sum(c__150) as Veg_and_Vegan,
    sum(c__152) as Vegetarian_and_Environmentally_friendly,
    sum(c__156) as Participative_economy,
    sum(c__160) as Internet_of_Things,
    sum(c__166) as E_Learning,
    sum(c__172) as Alzheimer_Aid,
    sum(c__82) as Music_Festivals,
    sum(c__176) as Phone,
    sum(c__178) as Charity,
    sum(c__182) as Bank_Brand_A,
    sum(c__72) as Reality_Television,
    sum(c__126) as Popular_Events,
    sum(c__127) as Snacks,
    sum(c__128) as Tapeo,
    sum(c__184) as Champions_League
    from
        (
        SELECT weboid,
        case when sd__8 is not null then 1 else 0 end as sd__8, 
        case when sd__6 is not null then 1 else 0 end as sd__6,
        case when sd__4 is not null then 1 else 0 end as sd__4,
        case when sd__7 is not null then 1 else 0 end as sd__7,
        case when sd__11 is not null then 1 else 0 end as sd__11,
        case when sd__12 is not null then 1 else 0 end as sd__12,
        case when sd__9 is not null then 1 else 0 end as sd__9,
        case when sd__5 is not null then 1 else 0 end as sd__5,
        case when sd__23 is not null then 1 else 0 end as sd__23,
        case when sd__22 is not null then 1 else 0 end as sd__22,
        case when sd__13 is not null then 1 else 0 end as sd__13,
        case when c__1  is not null then 1 else 0 end as c__1,
        case when c__2  is not null then 1 else 0 end as c__2,
        case when c__3  is not null then 1 else 0 end as c__3,
        case when c__5  is not null then 1 else 0 end as c__5,
        case when c__7  is not null then 1 else 0 end as c__7,
        case when c__9  is not null then 1 else 0 end as c__9,
        case when c__11  is not null then 1 else 0 end as c__11,
        case when c__13  is not null then 1 else 0 end as c__13,
        case when c__15  is not null then 1 else 0 end as c__15,
        case when c__16  is not null then 1 else 0 end as c__16,
        case when c__17  is not null then 1 else 0 end as c__17,
        case when c__19  is not null then 1 else 0 end as c__19,
        case when c__20  is not null then 1 else 0 end as c__20,
        case when c__23  is not null then 1 else 0 end as c__23,
        case when c__25  is not null then 1 else 0 end as c__25,
        case when c__27  is not null then 1 else 0 end as c__27,
        case when c__29  is not null then 1 else 0 end as c__29,
        case when c__30  is not null then 1 else 0 end as c__30,
        case when c__31  is not null then 1 else 0 end as c__31,
        case when c__32  is not null then 1 else 0 end as c__32,
        case when c__33  is not null then 1 else 0 end as c__33,
        case when c__35  is not null then 1 else 0 end as c__35,
        case when c__37  is not null then 1 else 0 end as c__37,
        case when c__39  is not null then 1 else 0 end as c__39,
        case when c__40  is not null then 1 else 0 end as c__40,
        case when c__41  is not null then 1 else 0 end as c__41,
        case when c__43  is not null then 1 else 0 end as c__43,
        case when c__45  is not null then 1 else 0 end as c__45,
        case when c__47  is not null then 1 else 0 end as c__47,
        case when c__49  is not null then 1 else 0 end as c__49,
        case when c__51  is not null then 1 else 0 end as c__51,
        case when c__52  is not null then 1 else 0 end as c__52,
        case when c__53  is not null then 1 else 0 end as c__53,
        case when c__54  is not null then 1 else 0 end as c__54,
        case when c__55  is not null then 1 else 0 end as c__55,
        case when c__56  is not null then 1 else 0 end as c__56,
        case when c__59  is not null then 1 else 0 end as c__59,
        case when c__60  is not null then 1 else 0 end as c__60,
        case when c__61  is not null then 1 else 0 end as c__61,
        case when c__63  is not null then 1 else 0 end as c__63,
        case when c__65  is not null then 1 else 0 end as c__65,
        case when c__67  is not null then 1 else 0 end as c__67,
        case when c__69  is not null then 1 else 0 end as c__69,
        case when c__71  is not null then 1 else 0 end as c__71,
        case when c__73  is not null then 1 else 0 end as c__73,
        case when c__75  is not null then 1 else 0 end as c__75,
        case when c__77  is not null then 1 else 0 end as c__77,
        case when c__79  is not null then 1 else 0 end as c__79,
        case when c__81  is not null then 1 else 0 end as c__81,
        case when c__83  is not null then 1 else 0 end as c__83,
        case when c__84  is not null then 1 else 0 end as c__84,
        case when c__85  is not null then 1 else 0 end as c__85,
        case when c__86  is not null then 1 else 0 end as c__86,
        case when c__87  is not null then 1 else 0 end as c__87,
        case when c__89  is not null then 1 else 0 end as c__89,
        case when c__90  is not null then 1 else 0 end as c__90,
        case when c__91  is not null then 1 else 0 end as c__91,
        case when c__92  is not null then 1 else 0 end as c__92,
        case when c__93  is not null then 1 else 0 end as c__93,
        case when c__95  is not null then 1 else 0 end as c__95,
        case when c__97  is not null then 1 else 0 end as c__97,
        case when c__99  is not null then 1 else 0 end as c__99,
        case when c__101  is not null then 1 else 0 end as c__101,
        case when c__102  is not null then 1 else 0 end as c__102,
        case when c__103  is not null then 1 else 0 end as c__103,
        case when c__105  is not null then 1 else 0 end as c__105,
        case when c__107  is not null then 1 else 0 end as c__107,
        case when c__108  is not null then 1 else 0 end as c__108,
        case when c__109  is not null then 1 else 0 end as c__109,
        case when c__110  is not null then 1 else 0 end as c__110,
        case when c__111  is not null then 1 else 0 end as c__111,
        case when c__112  is not null then 1 else 0 end as c__112,
        case when c__113  is not null then 1 else 0 end as c__113,
        case when c__115  is not null then 1 else 0 end as c__115,
        case when c__116  is not null then 1 else 0 end as c__116,
        case when c__117  is not null then 1 else 0 end as c__117,
        case when c__119  is not null then 1 else 0 end as c__119,
        case when c__120  is not null then 1 else 0 end as c__120,
        case when c__124  is not null then 1 else 0 end as c__124,
        case when c__125  is not null then 1 else 0 end as c__125,
        case when c__129  is not null then 1 else 0 end as c__129,
        case when c__131  is not null then 1 else 0 end as c__131,
        case when c__133  is not null then 1 else 0 end as c__133,
        case when c__134  is not null then 1 else 0 end as c__134,
        case when c__135  is not null then 1 else 0 end as c__135,
        case when c__137  is not null then 1 else 0 end as c__137,
        case when c__138  is not null then 1 else 0 end as c__138,
        case when c__139  is not null then 1 else 0 end as c__139,
        case when c__140  is not null then 1 else 0 end as c__140,
        case when c__147  is not null then 1 else 0 end as c__147,
        case when c__149  is not null then 1 else 0 end as c__149,
        case when c__151  is not null then 1 else 0 end as c__151,
        case when c__153  is not null then 1 else 0 end as c__153,
        case when c__154  is not null then 1 else 0 end as c__154,
        case when c__155  is not null then 1 else 0 end as c__155,
        case when c__157  is not null then 1 else 0 end as c__157,
        case when c__158  is not null then 1 else 0 end as c__158,
        case when c__159  is not null then 1 else 0 end as c__159,
        case when c__161  is not null then 1 else 0 end as c__161,
        case when c__162  is not null then 1 else 0 end as c__162,
        case when c__163  is not null then 1 else 0 end as c__163,
        case when c__164  is not null then 1 else 0 end as c__164,
        case when c__165  is not null then 1 else 0 end as c__165,
        case when c__167  is not null then 1 else 0 end as c__167,
        case when c__168  is not null then 1 else 0 end as c__168,
        case when c__169  is not null then 1 else 0 end as c__169,
        case when c__170  is not null then 1 else 0 end as c__170,
        case when c__171  is not null then 1 else 0 end as c__171,
        case when c__173  is not null then 1 else 0 end as c__173,
        case when c__174  is not null then 1 else 0 end as c__174,
        case when c__175  is not null then 1 else 0 end as c__175,
        case when c__177  is not null then 1 else 0 end as c__177,
        case when c__179  is not null then 1 else 0 end as c__179,
        case when c__180  is not null then 1 else 0 end as c__180,
        case when c__181  is not null then 1 else 0 end as c__181,
        case when c__183  is not null then 1 else 0 end as c__183,
        case when c__185  is not null then 1 else 0 end as c__185,
        case when c__187  is not null then 1 else 0 end as c__187,
        case when c__188  is not null then 1 else 0 end as c__188,
        case when c__189  is not null then 1 else 0 end as c__189,
        case when c__190  is not null then 1 else 0 end as c__190,
        case when c__191  is not null then 1 else 0 end as c__191,
        case when c__193  is not null then 1 else 0 end as c__193,
        case when c__195  is not null then 1 else 0 end as c__195,
        case when c__197  is not null then 1 else 0 end as c__197,
        case when c__199  is not null then 1 else 0 end as c__199,
        case when c__200  is not null then 1 else 0 end as c__200,
        case when c__201  is not null then 1 else 0 end as c__201,
        case when c__202  is not null then 1 else 0 end as c__202,
        case when c__203  is not null then 1 else 0 end as c__203,
        case when c__204  is not null then 1 else 0 end as c__204,
        case when c__205  is not null then 1 else 0 end as c__205,
        case when c__207  is not null then 1 else 0 end as c__207,
        case when c__208  is not null then 1 else 0 end as c__208,
        case when c__209  is not null then 1 else 0 end as c__209,
        case when c__210  is not null then 1 else 0 end as c__210,
        case when c__211  is not null then 1 else 0 end as c__211,
        case when c__212  is not null then 1 else 0 end as c__212,
        case when c__213  is not null then 1 else 0 end as c__213,
        case when c__215  is not null then 1 else 0 end as c__215,
        case when c__216  is not null then 1 else 0 end as c__216,
        case when c__217  is not null then 1 else 0 end as c__217,
        case when c__219  is not null then 1 else 0 end as c__219,
        case when c__220  is not null then 1 else 0 end as c__220,
        case when c__222  is not null then 1 else 0 end as c__222,
        case when c__223  is not null then 1 else 0 end as c__223,
        case when c__225  is not null then 1 else 0 end as c__225,
        case when c__227  is not null then 1 else 0 end as c__227,
        case when c__229  is not null then 1 else 0 end as c__229,
        case when c__231  is not null then 1 else 0 end as c__231,
        case when c__233  is not null then 1 else 0 end as c__233,
        case when c__235  is not null then 1 else 0 end as c__235,
        case when c__237  is not null then 1 else 0 end as c__237,
        case when c__239  is not null then 1 else 0 end as c__239,
        case when c__241  is not null then 1 else 0 end as c__241,
        case when c__243  is not null then 1 else 0 end as c__243,
        case when c__4  is not null then 1 else 0 end as c__4,
        case when c__6  is not null then 1 else 0 end as c__6,
        case when c__8  is not null then 1 else 0 end as c__8,
        case when c__10  is not null then 1 else 0 end as c__10,
        case when c__12  is not null then 1 else 0 end as c__12,
        case when c__14  is not null then 1 else 0 end as c__14,
        case when c__21  is not null then 1 else 0 end as c__21,
        case when c__22  is not null then 1 else 0 end as c__22,
        case when c__24  is not null then 1 else 0 end as c__24,
        case when c__26  is not null then 1 else 0 end as c__26,
        case when c__28  is not null then 1 else 0 end as c__28,
        case when c__18  is not null then 1 else 0 end as c__18,
        case when c__34  is not null then 1 else 0 end as c__34,
        case when c__36  is not null then 1 else 0 end as c__36,
        case when c__42  is not null then 1 else 0 end as c__42,
        case when c__44  is not null then 1 else 0 end as c__44,
        case when c__46  is not null then 1 else 0 end as c__46,
        case when c__38  is not null then 1 else 0 end as c__38,
        case when c__62  is not null then 1 else 0 end as c__62,
        case when c__50  is not null then 1 else 0 end as c__50,
        case when c__48  is not null then 1 else 0 end as c__48,
        case when c__142  is not null then 1 else 0 end as c__142,
        case when c__70  is not null then 1 else 0 end as c__70,
        case when c__144  is not null then 1 else 0 end as c__144,
        case when c__58  is not null then 1 else 0 end as c__58,
        case when c__74  is not null then 1 else 0 end as c__74,
        case when c__57  is not null then 1 else 0 end as c__57,
        case when c__66  is not null then 1 else 0 end as c__66,
        case when c__68  is not null then 1 else 0 end as c__68,
        case when c__64  is not null then 1 else 0 end as c__64,
        case when c__141  is not null then 1 else 0 end as c__141,
        case when c__80  is not null then 1 else 0 end as c__80,
        case when c__78  is not null then 1 else 0 end as c__78,
        case when c__76  is not null then 1 else 0 end as c__76,
        case when c__88  is not null then 1 else 0 end as c__88,
        case when c__94  is not null then 1 else 0 end as c__94,
        case when c__96  is not null then 1 else 0 end as c__96,
        case when c__98  is not null then 1 else 0 end as c__98,
        case when c__100  is not null then 1 else 0 end as c__100,
        case when c__104  is not null then 1 else 0 end as c__104,
        case when c__106  is not null then 1 else 0 end as c__106,
        case when c__114  is not null then 1 else 0 end as c__114,
        case when c__118  is not null then 1 else 0 end as c__118,
        case when c__121  is not null then 1 else 0 end as c__121,
        case when c__122  is not null then 1 else 0 end as c__122,
        case when c__123  is not null then 1 else 0 end as c__123,
        case when c__130  is not null then 1 else 0 end as c__130,
        case when c__132  is not null then 1 else 0 end as c__132,
        case when c__136  is not null then 1 else 0 end as c__136,
        case when c__146  is not null then 1 else 0 end as c__146,
        case when c__148  is not null then 1 else 0 end as c__148,
        case when c__150  is not null then 1 else 0 end as c__150,
        case when c__152  is not null then 1 else 0 end as c__152,
        case when c__156  is not null then 1 else 0 end as c__156,
        case when c__160  is not null then 1 else 0 end as c__160,
        case when c__166  is not null then 1 else 0 end as c__166,
        case when c__172  is not null then 1 else 0 end as c__172,
        case when c__82  is not null then 1 else 0 end as c__82,
        case when c__176  is not null then 1 else 0 end as c__176,
        case when c__178  is not null then 1 else 0 end as c__178,
        case when c__182  is not null then 1 else 0 end as c__182,
        case when c__72  is not null then 1 else 0 end as c__72,
        case when c__126  is not null then 1 else 0 end as c__126,
        case when c__127  is not null then 1 else 0 end as c__127,
        case when c__128  is not null then 1 else 0 end as c__128,
        case when c__184  is not null then 1 else 0 end as c__184
        FROM table_date_range([datamining-1184:WEBO_"""+a+""".wam_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""')))"""

    b = (query_BQ(count_w)).transpose()
    b.to_excel(writer, sheet_name='compte_' + str(liste_de_pays[i]),index=True, header =False)
    
writer.close()


# In[26]:


#os.system("gsutil cp report_wousdat_" + start_date_bq + "_" + end_date_bq + ".xlsx gs://luca_merzetti/report_wousdat_"+start_date_bq+"_"+end_date_bq+".xlsx")

# SEND REPORT TO STORAGE
# os.system("gsutil cp "+filepath+"report_wousdat_"+start_date_bq+"_"+end_date_bq+".xlsx gs://luca_merzetti/report_wousdat_"+start_date_bq+"_"+end_date_bq+".xlsx")
os.system("gsutil cp "+filepath+" gs://luca_merzetti/report_wousdat_"+start_date_bq+"_"+end_date_bq+".xlsx")

#change date format from YYYYMMDD to MM/DD/YYYY
start_old_format = datetime.datetime.strptime(start_date_bq,'%Y%m%d')
start_new_format = start_old_format.strftime('%m/%d/%Y')
end_old_format = datetime.datetime.strptime(end_date_bq,'%Y%m%d')
end_new_format = end_old_format.strftime('%m/%d/%Y')



# GET FILES NAMES IN STORAGE
def get_filenames(bucket_name):
        cmd = "gs://%s" %(bucket_name)

        filenames = subprocess.check_output('gsutil ls '+cmd, shell=True).decode("utf-8").split('\n')
        filenames_list = []

        for i in range(len(filenames)):
            filenames_list.append(filenames[i].replace(cmd,''))

        filenames_list = [x for x in filenames_list if x]

        return filenames_list

#setting variables for the different cases (success vs unsuccess)

receivers_success = ['jbougard@weborama.com', 'lmerzetti@weborama.com', 'cbusson@weborama.com']
receivers_unsuccess = ['lmerzetti@weborama.com', 'jbougard@weborama.com']

message_success = """Voici les comptages wousdat par cluster du """+start_new_format+""" au """+end_new_format+""":

https://console.cloud.google.com/storage/browser/luca_merzetti
"""

message_unsuccess = """ALERT!!! Le monthly report WOUSDAT du """+start_new_format+""" au """+end_new_format+""" n'est pas disponible. Something must have gone wrong, check the scripts!
Lien vers le bucket: https://console.cloud.google.com/storage/browser/luca_merzetti
"""

def monitoring_alert (task,receivers):
    sender = 'lmerzetti@weborama.com'
    
    for receiver in receivers:
        message = 'From: From Data Services <' + sender + '>\nTo: To Client <' + receiver + '>\nSubject: Wousdat monthly Report '+start_new_format+'-'+end_new_format+' \n\n' + task
        pwd = ''

        smtpObj = smtplib.SMTP('smtp.googlemail.com', 587)
        smtpObj.starttls()
        smtpObj.login('lmerzetti@weborama.com', pwd)
        smtpObj.sendmail(sender, receiver, message)
        smtpObj.quit()


def check_and_send(date_start, date_end, files_list):
    if "/report_wousdat_%s_%s.xlsx"%(date_start, date_end) in files_list:
        monitoring_alert(message_success,receivers_success)
    else:
        monitoring_alert(message_unsuccess,receivers_unsuccess)

fl = get_filenames("luca_merzetti")
# print(fl)

check_and_send(start_date_bq, end_date_bq, fl)