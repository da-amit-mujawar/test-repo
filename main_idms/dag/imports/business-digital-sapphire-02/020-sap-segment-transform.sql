-- remove empty values, create segments

delete from {table_sap_industry} where industry_m=' ';
delete from {table_sap_hardware} where technology_on_site_h=',HW0000,';
delete from {table_sap_software} where technology_on_site_s=',SW0000,';
delete from {table_sap_specialty} where specialty=' ';
delete from {table_sap_interestarea} where interest_area=' ';
delete from {table_sap_jobfunction} where job_function=' ';
delete from {table_sap_locationtype} where location_type=' ';
delete from {table_sap_productspecified} where product_specified=' ';

--Location_Type

insert into {table_location_type_segment}
  select individualmc,location_type
   from  {table_sap_locationtype}
   where       location_type like '%,LOC210,%'
           or location_type like '%,LOC215,%'
		   or location_type like '%,LOC211,%'
		   or location_type like '%,LOC213,%'
           or location_type like '%,LOC216,%'
		   or location_type like '%,LOC212,%'
		   or location_type like '%,LOC214,%'
		   or location_type like '%,LOC217,%'
		   or location_type like '%,LOC68,%';

insert into {table_job_stats}
select 'Sap Location Type segment qty',count(*),getdate() from {table_location_type_segment};

--Interest_Area

insert into {table_interest_area_segment}
  select individualmc,interest_area
  from  {table_sap_interestarea}
  where       interest_area like '%,INT172,%';

insert into {table_job_stats}
select 'Sap Interest area segment qty',count(*),getdate() from {table_interest_area_segment};

--Product_Specified

insert into {table_product_specified_segment}
  select individualmc,product_specified
  from  {table_sap_productspecified}
  where       product_specified like '%,P317,%'
           or product_specified like '%,P361,%'
		   or product_specified like '%,P105,%'
		   or product_specified like '%,P340,%'
           or product_specified like '%,P117,%'
		   or product_specified like '%,P330,%'
		   or product_specified like '%,P121,%'
		   or product_specified like '%,P124,%'
		   or product_specified like '%,P321,%'
		   or product_specified like '%,P112,%'
		   or product_specified like '%,P198,%'
           or product_specified like '%,P200,%'
		   or product_specified like '%,P339,%'
		   or product_specified like '%,P332,%'
		   or product_specified like '%,P336,%'
		   or product_specified like '%,P335,%'
		   or product_specified like '%,P324,%'
		   or product_specified like '%,P333,%'
           or product_specified like '%,P341,%'
		   or product_specified like '%,P337,%'
		   or product_specified like '%,P334,%'
		   or product_specified like '%,P353,%'
		   or product_specified like '%,P349,%'
		   or product_specified like '%,P351,%'
		   or product_specified like '%,P348,%'
           or product_specified like '%,P354,%'
		   or product_specified like '%,P113,%'
		   or product_specified like '%,P102,%'
		   or product_specified like '%,P108,%'
		   or product_specified like '%,P118,%'
		   or product_specified like '%,P119,%'
		   or product_specified like '%,P193,%'
           or product_specified like '%,P350,%'
		   or product_specified like '%,P139,%'
		   or product_specified like '%,P142,%'
		   or product_specified like '%,P127,%'
		   or product_specified like '%,P129,%'
		   or product_specified like '%,P302,%'
		   or product_specified like '%,P104,%'
           or product_specified like '%,P322,%'
		   or product_specified like '%,P330,%'
		   or product_specified like '%,P180,%'
		   or product_specified like '%,P144,%'
		   or product_specified like '%,P171,%'
           or product_specified like '%,P187,%'
		   or product_specified like '%,P166,%'
		   or product_specified like '%,P170,%'
		   or product_specified like '%,P331,%'
		   or product_specified like '%,P307,%'
		   or product_specified like '%,P158,%'
		   or product_specified like '%,P306,%'
           or product_specified like '%,P147,%'
		   or product_specified like '%,P156,%'
		   or product_specified like '%,P130,%'
	       or product_specified like '%,P114,%'
		   or product_specified like '%,P309,%'
	       or product_specified like '%,P188,%'
	       or product_specified like '%,P133,%'
		   or product_specified like '%,P143,%'
           or product_specified like '%,P311,%'
           or product_specified like '%,P308,%'
           or product_specified like '%,P314,%'
           or product_specified like '%,P305,%'
		   or product_specified like '%,P313,%'
	       or product_specified like '%,P179,%'
	       or product_specified like '%,P183,%';

insert into {table_job_stats}
select 'Sap Produsct specified segment qty',count(*),getdate() from {table_product_specified_segment};

--Job_function

insert into {table_job_function_segment}
  select individualmc,job_function
  from  {table_sap_jobfunction}
  where       job_function like '%,J208,%'
           or job_function like '%,J121,%'
           or job_function like '%,J120,%'
           or job_function like '%,J122,%'
           or job_function like '%,J305,%'
           or job_function like '%,J311,%'
           or job_function like '%,J312,%'
           or job_function like '%,J236,%'
           or job_function like '%,J229,%'
           or job_function like '%,J235,%'
           or job_function like '%,J237,%'
           or job_function like '%,J213,%'
           or job_function like '%,J290,%'
           or job_function like '%,J214,%'
           or job_function like '%,J220,%'
           or job_function like '%,J218,%'
           or job_function like '%,J300,%'
           or job_function like '%,J232,%'
           or job_function like '%,J362,%'
           or job_function like '%,J280,%'
           or job_function like '%,J101,%'
           or job_function like '%,J204,%'
           or job_function like '%,J123,%'
             or job_function like '%,J424,%'
             or job_function like '%,J360,%'
             or job_function like '%,J115,%'
             or job_function like '%,J275,%'
             or job_function like '%,J350,%'
             or job_function like '%,J132,%'
             or job_function like '%,J124,%'
             or job_function like '%,J425,%'
             or job_function like '%,J205,%'
             or job_function like '%,J427,%'
             or job_function like '%,J210,%'
             or job_function like '%,J352,%'
             or job_function like '%,J316,%'
             or job_function like '%,J352,%'
             or job_function like '%,J317,%'
             or job_function like '%,J101,%'
             or job_function like '%,J406,%'
             or job_function like '%,J100,%'
             or job_function like '%,J102,%'
             or job_function like '%,J351,%'
             or job_function like '%,J414,%'
             or job_function like '%,J250,%'
             or job_function like '%,J210,%'
             or job_function like '%,J402,%'
             or job_function like '%,J404,%'
             or job_function like '%,J310,%'
             or job_function like '%,J116,%'
             or job_function like '%,J115,%'
             or job_function like '%,J117,%'
             or job_function like '%,J275,%'
             or job_function like '%,J324,%'
             or job_function like '%,J251,%'
             or job_function like '%,J212,%'
             or job_function like '%,J402,%'
             or job_function like '%,J326,%'
             or job_function like '%,J416,%'
             or job_function like '%,J325,%'
             or job_function like '%,J421,%'
             or job_function like '%,J215,%'
             or job_function like '%,J216,%'
             or job_function like '%,J285,%'
             or job_function like '%,J415,%'
             or job_function like '%,J211,%'
             or job_function like '%,J327,%'
             or job_function like '%,J417,%'
             or job_function like '%,J403,%'
             or job_function like '%,J210,%'
             or job_function like '%,J310,%'
             or job_function like '%,J251,%'
             or job_function like '%,J250,%';

insert into {table_job_stats}
select 'Sap Job Function segment qty',count(*),getdate() from {table_job_function_segment};

--INDUSTRY_M

insert into {table_industry_m_segment}
  select individualmc,industry_m
  from  {table_sap_industry}
  where industry_m like '%,I303,%'
  or industry_m like '%,I304,%'
  or industry_m like '%,I305,%'
  or industry_m like '%,I107,%'
  or industry_m like '%,I223,%'
  or industry_m like '%,I366,%'
  or industry_m like '%,I367,%'
  or industry_m like '%,I407,%'
  or industry_m like '%,I308,%'
  or industry_m like '%,I301,%'
  or industry_m like '%,I323,%'
  or industry_m like '%,I307,%'
  or industry_m like '%,I406,%'
  or industry_m like '%,I321,%'
  or industry_m like '%,I330,%'
  or industry_m like '%,I119,%'
  or industry_m like '%,I319,%'
  or industry_m like '%,I320,%'
  or industry_m like '%,I112,%'
  or industry_m like '%,I113,%'
  or industry_m like '%,I202,%'
  or industry_m like '%,I117,%'
  or industry_m like '%,I354,%'
  or industry_m like '%,I390,%'
  or industry_m like '%,I214,%'
  or industry_m like '%,I315,%'
  or industry_m like '%,I117,%'
  or industry_m like '%,I332,%'
  or industry_m like '%,I322,%'
  or industry_m like '%,I386,%'
  or industry_m like '%,I326,%'
  or industry_m like '%,I391,%'
  or industry_m like '%,I116,%'
  or industry_m like '%,I221,%'
  or industry_m like '%,I222,%'
  or industry_m like '%,I218,%'
  or industry_m like '%,I208,%'
  or industry_m like '%,I331,%'
  or industry_m like '%,I106,%';

insert into {table_job_stats}
select 'Sap Industry M segment qty',count(*),getdate() from {table_industry_m_segment};

--Technology_On_Site_h

insert into {table_technology_on_site_h_segment}
  select individualmc,technology_on_site_h
  from  {table_sap_hardware}
  where technology_on_site_h like '%,HWS2005,%'
or technology_on_site_h like '%,HW1701,%'
or technology_on_site_h like '%,HW602,%'
or technology_on_site_h like '%,HW1268,%'
or technology_on_site_h like '%,HW979,%'
or technology_on_site_h like '%,HW773,%'
or technology_on_site_h like '%,HWS275,%'
or technology_on_site_h like '%,HSW280,%'
or technology_on_site_h like '%,HW966,%'
or technology_on_site_h like '%,HW967,%'
or technology_on_site_h like '%,HW968,%'
or technology_on_site_h like '%,HW817,%'
or technology_on_site_h like '%,HWS285,%'
or technology_on_site_h like '%,HW1222,%'
or technology_on_site_h like '%,HW1223,%'
or technology_on_site_h like '%,HW1224,%'
or technology_on_site_h like '%,HW1225,%'
or technology_on_site_h like '%,HW753,%'
or technology_on_site_h like '%,HW762,%'
or technology_on_site_h like '%,HW766,%'
or technology_on_site_h like '%,HW717,%'
or technology_on_site_h like '%,HW721,%'
or technology_on_site_h like '%,HW725,%'
or technology_on_site_h like '%,HW719,%'
or technology_on_site_h like '%,HW726,%'
or technology_on_site_h like '%,HW1279,%'
or technology_on_site_h like '%,HW1280,%'
or technology_on_site_h like '%,HW1219,%'
or technology_on_site_h like '%,HW755,%'
or technology_on_site_h like '%,HW714,%'
or technology_on_site_h like '%,HW1276,%'
or technology_on_site_h like '%,HW1218,%'
or technology_on_site_h like '%,HW758,%'
or technology_on_site_h like '%,HW711,%'
or technology_on_site_h like '%,HW1273,%'
or technology_on_site_h like '%,HW1274,%'
or technology_on_site_h like '%,HW1275,%'
or technology_on_site_h like '%,HW1220,%'
or technology_on_site_h like '%,HW1221,%'
or technology_on_site_h like '%,HW763,%'
or technology_on_site_h like '%,HW768,%'
or technology_on_site_h like '%,HW720,%'
or technology_on_site_h like '%,HW724,%'
or technology_on_site_h like '%,HW1277,%'
or technology_on_site_h like '%,HW1278,%'
or technology_on_site_h like '%,HW751,%'
or technology_on_site_h like '%,HW761,%'
or technology_on_site_h like '%,HW770,%'
or technology_on_site_h like '%,HW754,%'
or technology_on_site_h like '%,HW1241,%'
or technology_on_site_h like '%,HW1242,%'
or technology_on_site_h like '%,HW1243,%'
or technology_on_site_h like '%,HW1244,%'
or technology_on_site_h like '%,HW1226,%'
or technology_on_site_h like '%,HW1227,%'
or technology_on_site_h like '%,HW1228,%'
or technology_on_site_h like '%,HW1229,%'
or technology_on_site_h like '%,HW1249,%'
or technology_on_site_h like '%,HW1250,%'
or technology_on_site_h like '%,HW1251,%'
or technology_on_site_h like '%,HW1252,%'
or technology_on_site_h like '%,HW1030,%'
or technology_on_site_h like '%,HW1025,%'
or technology_on_site_h like '%,HW1028,%'
or technology_on_site_h like '%,HW1023,%'
or technology_on_site_h like '%,HW760,%'
or technology_on_site_h like '%,HW765,%'
or technology_on_site_h like '%,HW1245,%'
or technology_on_site_h like '%,HW1246,%'
or technology_on_site_h like '%,HW1247,%'
or technology_on_site_h like '%,HW1248,%'
or technology_on_site_h like '%,HW1230,%'
or technology_on_site_h like '%,HW1231,%'
or technology_on_site_h like '%,HW1253,%'
or technology_on_site_h like '%,HW1254,%'
or technology_on_site_h like '%,HW1255,%'
or technology_on_site_h like '%,HW1256,%'
or technology_on_site_h like '%,HW1026,%'
or technology_on_site_h like '%,HW1029,%'
or technology_on_site_h like '%,HW1024,%'
or technology_on_site_h like '%,HW1027,%'
or technology_on_site_h like '%,HW1232,%'
or technology_on_site_h like '%,HW1233,%'
or technology_on_site_h like '%,HW1234,%'
or technology_on_site_h like '%,HW1235,%'
or technology_on_site_h like '%,HW1236,%'
or technology_on_site_h like '%,HW1237,%'
or technology_on_site_h like '%,HW1238,%'
or technology_on_site_h like '%,HW1239,%'
or technology_on_site_h like '%,HW1265,%'
or technology_on_site_h like '%,HW1266,%'
or technology_on_site_h like '%,HW1267,%';

insert into {table_job_stats}
select 'Sap Technology Hardware segment qty',count(*),getdate() from {table_technology_on_site_h_segment};


--Technology_On_Site_s

insert into {table_technology_on_site_s_segment}
  select individualmc,technology_on_site_s
  from {table_sap_software}
  where    technology_on_site_s like '%,SHW808,%'
or technology_on_site_s like '%,SHW809,%'
or technology_on_site_s like '%,SHW811,%'
or technology_on_site_s like '%,SHW812,%'
or technology_on_site_s like '%,SHW813,%'
or technology_on_site_s like '%,SHW810,%'
or technology_on_site_s like '%,SWH810,%'
or technology_on_site_s like '%,SHW814,%'
or technology_on_site_s like '%,SHW815,%'
or technology_on_site_s like '%,SHW816,%'
or technology_on_site_s like '%,SWH1077,%'
or technology_on_site_s like '%,SW420,%'
or technology_on_site_s like '%,SHW1057,%'
or technology_on_site_s like '%,SHW1058,%'
or technology_on_site_s like '%,SHW1059,%'
or technology_on_site_s like '%,SHW1061,%'
or technology_on_site_s like '%,SHW1060,%'
or technology_on_site_s like '%,SHW1062,%'
or technology_on_site_s like '%,SHW1064,%'
or technology_on_site_s like '%,SHW1056,%'
or technology_on_site_s like '%,SHW1065,%'
or technology_on_site_s like '%,SHW106,%'
or technology_on_site_s like '%,SW949,%'
or technology_on_site_s like '%,SW785,%'
or technology_on_site_s like '%,SW786,%'
or technology_on_site_s like '%,SW824,%'
or technology_on_site_s like '%,SW1019,%'
or technology_on_site_s like '%,SW943,%'
or technology_on_site_s like '%,SW787,%'
or technology_on_site_s like '%,SW784,%'
or technology_on_site_s like '%,SW310,%'
or technology_on_site_s like '%,SW320,%'
or technology_on_site_s like '%,SW789,%'
or technology_on_site_s like '%,SW325,%'
or technology_on_site_s like '%,SW790,%'
or technology_on_site_s like '%,SW791,%'
or technology_on_site_s like '%,SW1012,%'
or technology_on_site_s like '%,SW792,%'
or technology_on_site_s like '%,SW280,%'
or technology_on_site_s like '%,SW793,%'
or technology_on_site_s like '%,SW936,%'
or technology_on_site_s like '%,SW937,%'
or technology_on_site_s like '%,SW938,%'
or technology_on_site_s like '%,SW285,%'
or technology_on_site_s like '%,SW794,%'
or technology_on_site_s like '%,SW788,%'
or technology_on_site_s like '%,SW340,%'
or technology_on_site_s like '%,SW350,%'
or technology_on_site_s like '%,SW290,%'
or technology_on_site_s like '%,SW300,%'
or technology_on_site_s like '%,SW360,%'
or technology_on_site_s like '%,SHW905,%'
or technology_on_site_s like '%,SHW906,%'
or technology_on_site_s like '%,SHW907,%'
or technology_on_site_s like '%,SHW908,%'
or technology_on_site_s like '%,SHW909,%'
or technology_on_site_s like '%,SHW910,%'
or technology_on_site_s like '%,SHW911,%'
or technology_on_site_s like '%,SHW903,%'
or technology_on_site_s like '%,SHW902,%'
or technology_on_site_s like '%,SHW912,%'
or technology_on_site_s like '%,SHW913,%'
or technology_on_site_s like '%,SHW914,%'
or technology_on_site_s like '%,SW897,%';

insert into {table_job_stats}
select 'Sap Technology Software segment qty',count(*),getdate() from {table_technology_on_site_s_segment};

--create bluekai segments

insert into {table_sap_bluekai}

select substring(a.individualmc,1,17) as individualmc,substring(a.listid,1,15) as listid,
       substring(a.daus_abinum,1,9) as daus_abinum,
        a.firstname,a.lastname,
	    a.addr1,a.addr2,
	    a.city,a.st,
	    a.zip,a.zip4,
	    a.deliverypoint,a.DeliveryPointDropInd,
case when (b.Location_Type like '%,LOC210,%'
           or b.Location_Type like '%,LOC215,%'
		   or b.Location_Type like '%,LOC211,%') then '1' end as SP001,
case when (b.Location_Type like '%,LOC213,%'
           or b.Location_Type like '%,LOC216,%'
		   or b.Location_Type like '%,LOC212,%'
		   or b.Location_Type like '%,LOC214,%'
		   or b.Location_Type like '%,LOC217,%') then '1' end as SP002,
case when (c.Product_Specified like '%,P317,%'
           or c.Product_Specified like '%,P361,%'
		   or c.Product_Specified like '%,P105,%'
		   or c.Product_Specified like '%,P340,%') then '1' end as SP031,
case when (c.Product_Specified like '%,P124,%'
           or c.Product_Specified like '%,P321,%'
		   or c.Product_Specified like '%,P112,%'
		   or c.Product_Specified like '%,P198,%'
		   or c.Product_Specified like '%,P200,%'
		   or c.Product_Specified like '%,P339,%'
		   or c.Product_Specified like '%,P332,%'
		   or c.Product_Specified like '%,P336,%'
		   or c.Product_Specified like '%,P335,%'
		   or c.Product_Specified like '%,P324,%'
		   or c.Product_Specified like '%,P333,%'
		   or c.Product_Specified like '%,P341,%'
		   or c.Product_Specified like '%,P337,%'
		   or c.Product_Specified like '%,P334,%'
		   or c.Product_Specified like '%,P353,%') then '1' end as SP040,
case when (c.Product_Specified like '%,P349,%'
           or c.Product_Specified like '%,P351,%'
		   or c.Product_Specified like '%,P348,%'
		   or c.Product_Specified like '%,P354,%'
		   or c.Product_Specified like '%,P113,%') then '1' end as SP041,
case when (c.Product_Specified like '%,P102,%'
           or c.Product_Specified like '%,P349,%'
		   or c.Product_Specified like '%,P108,%'
		   or c.Product_Specified like '%,P351,%'
		   or c.Product_Specified like '%,P348,%'
		   or c.Product_Specified like '%,P354,%'
		   or c.Product_Specified like '%,P113,%'
		   or c.Product_Specified like '%,P118,%'
		   or c.Product_Specified like '%,P119,%') then '1' end as SP043,
case when (c.Product_Specified like '%,P117,%'
           or c.Product_Specified like '%,P330,%'
		   or c.Product_Specified like '%,P121,%') then '1' end as SP045,
case when (e.Industry_m like '%,I303,%'
           or e.Industry_m like '%,I304,%'
		   or e.Industry_m like '%,I305,%'
		   or e.Industry_m like '%,I107,%')
		   or d.Job_Function like '%,J208,%' then '1' end as SP125,
case when (c.Product_Specified like '%,P193,%'
           or c.Product_Specified like '%,P350,%'
		   or c.Product_Specified like '%,P139,%'
		   or c.Product_Specified like '%,P142,%'
		   or c.Product_Specified like '%,P127,%'
		   or c.Product_Specified like '%,P129,%'
		   or c.Product_Specified like '%,P302,%'
		   or c.Product_Specified like '%,P104,%'
		   or c.Product_Specified like '%,P322,%'
		   or c.Product_Specified like '%,P141,%'
		   or c.Product_Specified like '%,P330,%'
		   or c.Product_Specified like '%,P180,%'
		   or c.Product_Specified like '%,P144,%'
		   or c.Product_Specified like '%,P171,%'
		   or c.Product_Specified like '%,P187,%'
		   or c.Product_Specified like '%,P166,%'
		   or c.Product_Specified like '%,P170,%'
		   or c.Product_Specified like '%,P331,%'
		   or c.Product_Specified like '%,P307,%'
		   or c.Product_Specified like '%,P158,%'
		   or c.Product_Specified like '%,P306,%'
		   or c.Product_Specified like '%,P147,%'
		   or c.Product_Specified like '%,P156,%') then '1' end as SP134,
case when (d.Job_Function like '%,J121,%'
           or d.Job_Function like '%,J120,%'
		   or d.Job_Function like '%,J122,%'
		   or d.Job_Function like '%,J305,%'
		   or d.Job_Function like '%,I311,%')
		   or a.Text_job_Title='ZM' then '1' end as SP136,
case when (d.Job_Function like '%,J312,%'
           or d.Job_Function like '%,J236,%'
		   or d.Job_Function like '%,J229,%'
		   or d.Job_Function like '%,J235,%'
		   or d.Job_Function like '%,I237,%'
		   or d.Job_Function like '%,J213,%'
		   or d.Job_Function like '%,J290,%'
		   or d.Job_Function like '%,I214,%') then '1' end as SP143,
case when a.Text_job_Title='ZG'
                or a.Employees_Combined='ES05'
				or a.Text_Title_Function_One_Click='FO'
				or a.Text_JobTitle='FO'
				or a.Industry_p='LOC68' then '1' end as SP144,
case when d.Job_Function like '%,J220,%' then '1' end as SP146,
case when (d.Job_Function like '%,J218,%'
           or d.Job_Function like '%,J300,%') then '1' end as SP147,
case when (d.Job_Function like '%,J232,%'
           or d.Job_Function like '%,J362,%'
		   or d.Job_Function like '%,J280,%') then '1' end as SP149,
case when a.Text_Title_Function_One_Click
      in ('AD','L0','AR','AS','AC','BD','U0','D0','CS','CO','J0','DP','R0','N0','EX','S0','A0','F0','B0','K0','V0',
	      'JU','G0','MT','Y0','I0','E0','MM','MI','OP','OF','Q0','X0','C0','M0','Z0','P0','RE','H0','W0','SR','T0',
		  'ST','SC','O0')
	  then '1' end as SP237,
case when f.Technology_On_Site_h like '%,HW1701,%'   then '1' end as SP239,
case when (g.Technology_On_Site_s like '%,SHW808,%'
           or g.Technology_On_Site_s like '%,SHW809,%'
		   or g.Technology_On_Site_s like '%,SHW811,%'
		   or g.Technology_On_Site_s like '%,SHW812,%'
		   or g.Technology_On_Site_s like '%,SHW813,%'
		   or g.Technology_On_Site_s like '%,SHW810,%'
		   or g.Technology_On_Site_s like '%,SHW814,%'
		   or g.Technology_On_Site_s like '%,SHW815,%'
		   or g.Technology_On_Site_s like '%,SHW816,%') then '1' end as SP241,
case when (f.Technology_On_Site_h like '%,HW979,%'
           or f.Technology_On_Site_h like '%,HW773,%'
		   or f.Technology_On_Site_h like '%,HWS275,%'
		   or f.Technology_On_Site_h like '%,HSW280,%'
		   or f.Technology_On_Site_h like '%,HW966,%'
		   or f.Technology_On_Site_h like '%,HW967,%'
		   or f.Technology_On_Site_h like '%,HW968,%'
		   or f.Technology_On_Site_h like '%,HW817,%'
		   or f.Technology_On_Site_h like '%,HWS285,%'
		   or g.Technology_On_Site_s like '%SW949,%'
or g.Technology_On_Site_s like '%,SW785,%'
or g.Technology_On_Site_s like '%,SW786,%'
or g.Technology_On_Site_s like '%,SW824,%'
or g.Technology_On_Site_s like '%,SW1019,%'
or g.Technology_On_Site_s like '%,SW943,%'
or g.Technology_On_Site_s like '%,SW787,%'
or g.Technology_On_Site_s like '%,SW784,%'
or g.Technology_On_Site_s like '%,SW310,%'
or g.Technology_On_Site_s like '%,SW320,%'
or g.Technology_On_Site_s like '%,SW789,%'
or g.Technology_On_Site_s like '%,SW325,%'
or g.Technology_On_Site_s like '%,SW790,%'
or g.Technology_On_Site_s like '%,SW791,%'
or g.Technology_On_Site_s like '%,SW1012,%'
or g.Technology_On_Site_s like '%,SW792,%'
or g.Technology_On_Site_s like '%,SW280,%'
or g.Technology_On_Site_s like '%,SW793,%'
or g.Technology_On_Site_s like '%,SW936,%'
or g.Technology_On_Site_s like '%,SW937,%'
or g.Technology_On_Site_s like '%,SW938,%'
or g.Technology_On_Site_s like '%,SW285,%'
or g.Technology_On_Site_s like '%,SW794,%'
or g.Technology_On_Site_s like '%,SW788,%'
or g.Technology_On_Site_s like '%,SW340,%'
or g.Technology_On_Site_s like '%,SW350,%'
or g.Technology_On_Site_s like '%,SW290,%'
or g.Technology_On_Site_s like '%,SW300,%'
or g.Technology_On_Site_s like '%,SW360,%') then '1' end as SP242,
case when f.Technology_On_Site_h like '%,HW602,%'   then '1' end as SP243,
case when (g.Technology_On_Site_s like '%,SHW905,%'
                 or g.Technology_On_Site_s like '%,SHW906,%'
				 or g.Technology_On_Site_s like '%,SHW907,%'
				 or g.Technology_On_Site_s like '%,SHW908,%'
				 or g.Technology_On_Site_s like '%,SHW909,%'
				 or g.Technology_On_Site_s like '%,SHW910,%'
				 or g.Technology_On_Site_s like '%,SHW911,%'
				 or g.Technology_On_Site_s like '%,SHW903,%'
				 or g.Technology_On_Site_s like '%,SHW902,%'
				 or g.Technology_On_Site_s like '%,SHW912,%'
				 or g.Technology_On_Site_s like '%,SHW913,%'
				 or g.Technology_On_Site_s like '%,SHW914,%'
				 or g.Technology_On_Site_s like '%,SW897,%') then '1' end as SP245,
case when (f.Technology_On_Site_h like '%,HW1222,%'
           or f.Technology_On_Site_h like '%,HW1223,%'
		   or f.Technology_On_Site_h like '%,HW1224,%'
		   or f.Technology_On_Site_h like '%,HW1225,%'
		   or f.Technology_On_Site_h like '%,HW753,%'
		   or f.Technology_On_Site_h like '%,HW762,%'
		   or f.Technology_On_Site_h like '%,HW766,%'
		   or f.Technology_On_Site_h like '%,HW717,%'
		   or f.Technology_On_Site_h like '%,HW721,%'
		   or f.Technology_On_Site_h like '%,HW725,%'
		   or f.Technology_On_Site_h like '%,HW719,%'
		   or f.Technology_On_Site_h like '%,HW726,%'
		   or f.Technology_On_Site_h like '%,HW1279,%'
		   or f.Technology_On_Site_h like '%,HW1280,%') then '1' end as SP247,
case when (f.Technology_On_Site_h like '%,HW1219,%'
           or f.Technology_On_Site_h like '%,HW755,%'
		   or f.Technology_On_Site_h like '%,HW714,%'
		   or f.Technology_On_Site_h like '%,HW1276,%') then '1' end as SP248,
case when (f.Technology_On_Site_h like '%,HW1218,%'
           or f.Technology_On_Site_h like '%,HW758,%'
		   or f.Technology_On_Site_h like '%,HW711,%'
		   or f.Technology_On_Site_h like '%,HW1273,%'
		   or f.Technology_On_Site_h like '%,HW1274,%'
		   or f.Technology_On_Site_h like '%,HW1275,%') then '1' end as SP249,
case when (f.Technology_On_Site_h like '%,HW1220,%'
           or f.Technology_On_Site_h like '%,HW1221,%'
		   or f.Technology_On_Site_h like '%,HW763,%'
		   or f.Technology_On_Site_h like '%,HW768,%'
		   or f.Technology_On_Site_h like '%,HW720,%'
		   or f.Technology_On_Site_h like '%,HW724,%'
		   or f.Technology_On_Site_h like '%,HW1277,%'
		   or f.Technology_On_Site_h like '%,HW1278,%') then '1' end as SP250,
case when ( f.Technology_On_Site_h like '%,HW751,%'
	or f.Technology_On_Site_h like '%,HW761,%'
	or f.Technology_On_Site_h like '%,HW770,%'
	or f.Technology_On_Site_h like '%,HW754,%'
	or f.Technology_On_Site_h like '%,HW1241,%'
	or f.Technology_On_Site_h like '%,HW1242,%'
	or f.Technology_On_Site_h like '%,HW1243,%'
	or f.Technology_On_Site_h like '%,HW1244,%'
	or f.Technology_On_Site_h like '%,HW1226,%'
	or f.Technology_On_Site_h like '%,HW1227,%'
	or f.Technology_On_Site_h like '%,HW1228,%'
	or f.Technology_On_Site_h like '%,HW1229,%'
	or f.Technology_On_Site_h like '%,HW1249,%'
	or f.Technology_On_Site_h like '%,HW1250,%'
	or f.Technology_On_Site_h like '%,HW1251,%'
	or f.Technology_On_Site_h like '%,HW1252,%'
	or f.Technology_On_Site_h like '%,HW1030,%'
	or f.Technology_On_Site_h like '%,HW1025,%'
	or f.Technology_On_Site_h like '%,HW1028,%'
	or f.Technology_On_Site_h like '%,HW1023,%') then '1' end as SP251,
case when (f.Technology_On_Site_h like '%,HW760,%'
	or f.Technology_On_Site_h like '%,HW765,%'
	or f.Technology_On_Site_h like '%,HW1245,%'
	or f.Technology_On_Site_h like '%,HW1246,%'
	or f.Technology_On_Site_h like '%,HW1247,%'
	or f.Technology_On_Site_h like '%,HW1248,%'
	or f.Technology_On_Site_h like '%,HW1230,%'
	or f.Technology_On_Site_h like '%,HW1231,%'
	or f.Technology_On_Site_h like '%,HW1253,%'
	or f.Technology_On_Site_h like '%,HW1254,%'
	or f.Technology_On_Site_h like '%,HW1255,%'
	or f.Technology_On_Site_h like '%,HW1256,%'
	or f.Technology_On_Site_h like '%,HW1026,%'
	or f.Technology_On_Site_h like '%,HW1029,%'
	or f.Technology_On_Site_h like '%,HW1024,%'
	or f.Technology_On_Site_h like '%,HW1027,%') then '1' end as SP252,
case when (f.Technology_On_Site_h like '%,HW1232,%'
	or f.Technology_On_Site_h like '%,HW1233,%'
	or f.Technology_On_Site_h like '%,HW1234,%'
	or f.Technology_On_Site_h like '%,HW1235,%') then '1' end as SP253,
case when (f.Technology_On_Site_h like '%,HW1236,%'
	or f.Technology_On_Site_h like '%,HW1237,%'
	or f.Technology_On_Site_h like '%,HW1238,%'
	or f.Technology_On_Site_h like '%,HW1239,%') then '1' end as SP254,
case when f.Technology_On_Site_h like '%,HW1268,%' then '1' end as SP256,
case when (f.Technology_On_Site_h like '%,HW1265,%'
	or f.Technology_On_Site_h like '%,HW1266,%'
	or f.Technology_On_Site_h like '%,HW1267,%') then '1' end as SP257,
case when (g.Technology_On_Site_s like '%,SHW1057,%'
                 or g.Technology_On_Site_s like '%,SHW1058,%'
				 or g.Technology_On_Site_s like '%,SHW1059,%'
				 or g.Technology_On_Site_s like '%,SHW1060,%'
				 or g.Technology_On_Site_s like '%,SHW1061,%'
				 or g.Technology_On_Site_s like '%,SHW1062,%'
				 or g.Technology_On_Site_s like '%,SHW1064,%'
				 or g.Technology_On_Site_s like '%,SHW1056,%'
				 or g.Technology_On_Site_s like '%,SHW1065,%'
				 or g.Technology_On_Site_s like '%,SHW106,%') then '1' end as SP259,
case when (g.Technology_On_Site_s like '%,SWH1077,%'
                 or g.Technology_On_Site_s like '%,SW420,%') then '1' end as SP261,
case when (c.Product_Specified like '%,P130,%'
           or c.Product_Specified like '%,P114,%'
		   or c.Product_Specified like '%,P309,%'
		   or c.Product_Specified like '%,P188,%'
		   or c.Product_Specified like '%,P133,%') then '1' end as SP030,
case when (c.Product_Specified like '%,P143,%'
           or c.Product_Specified like '%,P311,%'
		   or c.Product_Specified like '%,P308,%'
		   or c.Product_Specified like '%,P314,%'
		   or c.Product_Specified like '%,P305,%') then '1' end as SP036,
case when (c.Product_Specified like '%,P313,%'
           or c.Product_Specified like '%,P179,%'
		   or c.Product_Specified like '%,P308,%'
		   or c.Product_Specified like '%,P183,%') then '1' end as SP037,
case when d.Job_Function like '%,J101,%'
                or a.Text_Title_Function_One_Click
      in ('EX','CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0','CH_N0','CH_S0','CH_A0','CH_F0','CH_B0',
          'CH_K0','CH_V0','CH_G0','CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0','CH_M0','CH_Z0',
          'CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0')
      OR a.Text_JobTitle in ('BO','CH','CM','EX','FO','OF')
      OR a.Text_job_Title in('V','V31','Y','Y31','Z','ZA','ZB','ZC','ZD','ZE','ZF','ZM') then '1' end as SP047,
case when
(
(d.Job_Function like '%,J204,%'
or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%'
)
or
a.Text_Title_Function_One_Click
 in ('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')

 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')

 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM')
)
and
(
(e.Industry_M like '%,I223,%'
  or e.Industry_M like '%,I366,%'
  or e.Industry_M like '%,I367,%'
  or e.Industry_M like '%,I407,%'
  or e.Industry_M like '%,I308,%'
  or e.Industry_M like '%,I301,%'
  or e.Industry_M like '%,I323,%'
  or e.Industry_M like '%,I307,%'
  or e.Industry_M like '%,I406,%')
  OR
  a.SIC2 in('46','49')
  OR
  a.Industry_p in('I223','I366','I367','I407','I308','I301','I323','I307','I406')
 )
  then '1' end as SP049,

case when(
(d.Job_Function like '%,J204,%'
or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM')
)
and
(
(e.Industry_M like '%,I340,%'
 or e.Industry_M like '%,I355,%'
 or e.Industry_M like '%,I402,%')
 OR
 a.SIC2 in('78','79')
 OR
 a.Industry_p in('I340','I355','I402')
 OR
 h.Interest_Area like '%,INT172,%'
 )  then '1' end as SP050,

case when
(
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0','CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0',
    'CH_G0','CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0','CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0',
    'CH_SC','CH_O0')
 OR a.Text_JobTitle in('BO','CH','EX','FO','OF')
 OR a.Text_job_Title in('Y','Y31','ZA','ZB','ZC','ZD','ZE','ZF','ZM')
 )
and
  (b.Location_Type like '%,LOC68,%'
  OR
  a.ResBus_Indicator in('R','U')
  OR
  a.Address_Type='C')  then '1' end as SP051,

case when
(
(d.Job_Function like '%,J204,%'
or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM')
)
and
((e.Industry_M like '%,I321,%'
  or e.Industry_M like '%,I330,%'
  or e.Industry_M like '%,I119,%'
  or e.Industry_M like '%,I319,%'
  or e.Industry_M like '%,I320,%'
  or e.Industry_M like '%,I112,%'
  or e.Industry_M like '%,I113,%')
  OR
  a.SIC2 in('60','61','62','63','64','65','67','93')
  OR
  a.Industry_p in('I321','I330','I119','I319','I320','I112','I113')
  OR
  a.Text_Title_Function_One_Click in('CH_A0','CM_A0','SU_A0'))  then '1' end as SP052,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM'))
and
((e.Industry_M like '%,I202,%'
  or e.Industry_M like '%,I117,%'
  or e.Industry_M like '%,I354,%'
  or e.Industry_M like '%,I390,%'
  or e.Industry_M like '%,I214,%')
  OR
  a.Industry_p in('I202','I117','I354','I390','I214')) then '1' end as SP054,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM'))
and
( e.Industry_M like '%,I315,%'
  OR
  a.SIC2 in('72','73')
  OR
  a.Industry_p='I315'
  OR
  a.Text_Title_Function_One_Click in('CH_B0','CH_O0','B0','CM_B0','SU_B0','SU_O0','O0'))  then '1' end as SP055,

case when a.Text_Title_Function_One_Click in('CH_V0','CM_V0','SU_V0')
  OR a.Text_job_Title in('ZE','ZF')   then '1' end as SP056,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM'))
and a.Employees_Combined in('ES24','ES0F','ES29','ES25') then '1' end as SP057,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0','CH_N0','CH_S0',
'CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0','CH_MT','CH_Y0','CH_I0','CH_E0',
'CH_OP','CH_Q0','CH_X0','CH_C0','CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0',
'CH_SC','CH_O0','CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0','CM_MT','CM_Y0',
'CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0','CM_M0','CM_Z0','CM_P0','CM_H0',
'CM_W0','CM_T0','CM_SC','CM_O0','SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS',
'SU_J0','SU_R0','SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0','SU_M0','SU_Z0',
'SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10','A11','A12',
                  'A13','A14','A15','A16','A17','A18','A19','A20','A21','A22','A23',
                  'A24','A25','A26','A27','A28','A29','A30','A31','C01','C02','C02',
                  'C03','C05','C06','X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD',
                  'ZE','ZF','ZM'))
                  and a.Gender='M' then '1' end as SP062,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM'))
and
((e.Industry_M like '%,I117,%'
  or e.Industry_M like '%,I332,%'
  or e.Industry_M like '%,I322,%'
  or e.Industry_M like '%,I386,%'
  or e.Industry_M like '%,I326,%'
  or e.Industry_M like '%,I391,%'
  or e.Industry_M like '%,I116,%')
  OR
  a.Industry_p in('I117','I332','I322','I386','I326','I391','I116')
  OR
  a.Text_Title_Function_One_Click in('CH_E0', 'CM_E0', 'SU_E0')
  OR
  (d.Job_Function like '%,J250,%'
   or d.Job_Function like '%,J210,%'
   or d.Job_Function like '%,J402,%'
   or d.Job_Function like '%,J404,%'
   or d.Job_Function like '%,J310,%'))  then '1' end as SP060,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM'))
and
((e.Industry_M like '%,I203,%'
or e.Industry_M like '%,I383,%'
or e.Industry_M like '%,I385,%'
or e.Industry_M like '%,I382,%'
or e.Industry_M like '%,I352,%'
or e.Industry_M like '%,I204,%')
  OR
  a.SIC2 in('52', '53', '54', '55', '56', '57', '58', '59')
  OR
  a.Industry_p in('I203', 'I383', 'I385', 'I382', 'I352', 'I204')) then '1' end as SP063,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM'))
and
((e.Industry_M like '%,I221,%'
  or e.Industry_M like '%,I222,%'
  or e.Industry_M like '%,I218,%'
  or e.Industry_M like '%,I208,%'
  or e.Industry_M like '%,I331,%'
  or e.Industry_M like '%,I106,%')
  OR
  a.Industry_p in('I221','I222','I218','I208','I331','I106')
  OR
  a.SIC2='73'
  OR
  a.Text_Title_Function_One_Click in('CH_N0','CH_V0','CM_N0','CM_V0', 'SU_N0', 'SU_V0')) then '1' end as SP066,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM'))
and a.Employees_Combined in('ES04','ES03', 'ES02', 'ES01', 'ES0A', 'ES06', 'ES07', 'ES08', 'ES09', 'ES10', 'ES11',
 'ES0D', 'ES12') then '1' end as SP067,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0','CH_N0','CH_S0',
'CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0','CH_MT','CH_Y0','CH_I0','CH_E0',
'CH_OP','CH_Q0','CH_X0','CH_C0','CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0',
'CH_SC','CH_O0','CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0','CM_MT','CM_Y0',
'CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0','CM_M0','CM_Z0','CM_P0','CM_H0',
'CM_W0','CM_T0','CM_SC','CM_O0','SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS',
'SU_J0','SU_R0','SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0','SU_M0','SU_Z0',
'SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10','A11','A12',
                  'A13','A14','A15','A16','A17','A18','A19','A20','A21','A22','A23',
                  'A24','A25','A26','A27','A28','A29','A30','A31','C01','C02','C02',
                  'C03','C05','C06','X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD',
                  'ZE','ZF','ZM'))
                  and a.Gender='F' then '1' end as SP068,
case when
         (d.Job_Function like '%,J116,%'
	  or d.Job_Function like '%,J115,%'
	  or d.Job_Function like '%,J117,%'
	  or d.Job_Function like '%,J275,%'
	  or d.Job_Function like '%,J324,%')
OR
a.Text_Job_Title='ZD'   then '1' end as SP129,
case when
       (  d.Job_Function like '%,J251,%'
	  or d.Job_Function like '%,J212,%'
	  or d.Job_Function like '%,J402,%'
	  or d.Job_Function like '%,J326,%'
	  or d.Job_Function like '%,J416,%'
	  or d.Job_Function like '%,J325,%')  then '1' end as SP138,
case when
        ( d.Job_Function like '%,J421,%'
	  or d.Job_Function like '%,J215,%'
	  or d.Job_Function like '%,J216,%'
	  or d.Job_Function like '%,J285,%')
	  OR
 a.Industry_p in('I368','I102','I374','I215','I306','I351','I106','I373','I361','I224','I311','I310',
 'I114','I371','I313','I302','I121','I370','I369','I125','I365','I211','I309','I312')  then '1' end as SP142,
case when a.Gender='F'
      then '1' end as SP069,
case when a.Gender='M'
      then '1' end as SP070,
case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM'))
and
  a.Employees_Combined in('ES14','ES15', 'ES0E', 'ES16', 'ES17', 'ES19', 'ES18', 'ES0C', 'ES20') then '1' end as SP061,

case when((d.Job_Function like '%,J204,%'
      or d.Job_Function like '%,J123,%'
or d.Job_Function like '%,J424,%'
or d.Job_Function like '%,J360,%'
or d.Job_Function like '%,J115,%'
or d.Job_Function like '%,J275,%'
or d.Job_Function like '%,J350,%'
or d.Job_Function like '%,J132,%'
or d.Job_Function like '%,J124,%'
or d.Job_Function like '%,J425,%'
or d.Job_Function like '%,J205,%'
or d.Job_Function like '%,J427,%'
or d.Job_Function like '%,J210,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J316,%'
or d.Job_Function like '%,J352,%'
or d.Job_Function like '%,J317,%'
or d.Job_Function like '%,J101,%'
or d.Job_Function like '%,J406,%'
or d.Job_Function like '%,J100,%'
or d.Job_Function like '%,J102,%'
or d.Job_Function like '%,J351,%'
or d.Job_Function like '%,J414,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0',
'CH_N0','CH_S0','CH_A0','CH_F0','CH_B0','CH_K0','CH_V0','CH_G0',
'CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0'
,'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0',
'CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0','CM_V0','CM_G0',
'CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0',
'CM_M0','CM_Z0','CM_P0','CM_H0','CM_W0','CM_T0','CM_SC','CM_O0',
'SU','SU_L0','SU_BD','SU_U0','SU_D0','SU_CS','SU_J0','SU_R0',
'SU_N0','SU_S0','SU_A0','SU_F0','SU_B0','SU_K0','SU_V0','SU_G0',
'SU_MT','SU_Y0','SU_I0','SU_E0','SU_OP','SU_Q0','SU_X0','SU_C0',
'SU_M0','SU_Z0','SU_P0','SU_H0','SU_W0','SU_T0','SU_SC','SU_O0')
 OR a.Text_JobTitle in ('BO','CH','CM','EX','MG','OF','SU')
 OR a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10',
               'A11','A12','A13','A14','A15','A16','A17','A18','A19',
               'A20','A21','A22','A23','A24','A25','A26','A27','A28',
               'A29','A30','A31','C01','C02','C02','C03','C05','C06',
               'X01','X03','X06','Y','Y31','ZA','ZB','ZC','ZD','ZE',
               'ZF','ZM'))
and
  a.Employees_Combined='ES13' then '1' end as SP065,


case when(d.Job_Function like '%,J100,%'
       or d.Job_Function like '%,J101,%'
       or d.Job_Function like '%,J115,%'
       or d.Job_Function like '%,J124,%'
	   or d.Job_Function like '%,J102,%')
or
a.Text_Title_Function_One_Click
 in('CH','CH_L0','CH_BD','CH_U0','CH_D0','CH_CS','CH_J0','CH_R0','CH_N0','CH_S0','CH_A0','CH_F0',
'CH_B0','CH_K0','CH_V0','CH_G0','CH_MT','CH_Y0','CH_I0','CH_E0','CH_OP','CH_Q0','CH_X0','CH_C0',
'CH_M0','CH_Z0','CH_P0','CH_H0','CH_W0','CH_T0','CH_SC','CH_O0',
'CM','CM_L0','CM_BD','CM_U0','CM_D0','CM_CS','CM_J0','CM_R0','CM_N0','CM_S0','CM_A0','CM_F0','CM_B0','CM_K0',
'CM_V0','CM_G0','CM_MT','CM_Y0','CM_I0','CM_E0','CM_OP','CM_Q0','CM_X0','CM_C0','CM_M0','CM_Z0','CM_P0',
'CM_H0','CM_W0','CM_T0','CM_SC','CM_O0')
OR
 a.Text_job_Title in('A01','A02','A03','A04','A05','A06','A07','A08','A10','A11','A12','A13','A14','A15','A16','A17','A18',
'A19','A20','A21','A22','A23','A24','A25','A26','A27','A28','A29','A30','A31') then '1' end as SP277,

case when(d.Job_Function like '%,J415,%'
       or d.Job_Function like '%,J211,%'
       or d.Job_Function like '%,J327,%'
       or d.Job_Function like '%,J417,%'
	   or d.Job_Function like '%,J403,%'
	   or d.Job_Function like '%,J210,%'
	   or d.Job_Function like '%,J310,%'
	   or d.Job_Function like '%,J251,%'
	   or d.Job_Function like '%,J250,%')
or a.Industry_p ='I322' then '1' end as SP276
from {table_sap_main} a
left outer join {table_location_type_segment} b
on a.individualmc=b.individualmc
left outer join {table_product_specified_segment} c
on a.individualmc=c.individualmc
left outer join {table_job_function_segment} d
on a.individualmc=d.individualmc
left outer join {table_industry_m_segment} e
on a.individualmc=e.individualmc
left outer join {table_technology_on_site_h_segment} f
on a.individualmc=f.individualmc
left outer join {table_technology_on_site_s_segment} g
on a.individualmc=g.individualmc
left outer join {table_interest_area_segment} h
on a.individualmc=h.individualmc;
--129616518

insert into {table_job_stats}
select 'Sap Bluekai segment qty',count(*),getdate() from {table_sap_bluekai};