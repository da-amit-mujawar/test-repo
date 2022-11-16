/*
    Copied from jayesh sample. Reju Mathew. Jira 626
    2021.01.25
*/
create or replace function  udf_getlocation(tcCountryName varchar(max))
  returns char(1)
stable
as $$
	if tcCountryName is None:
		tcCountryName = ''
	lcReturnValue = ''
	CountryName = tcCountryName.strip().upper()
	if CountryName == 'USA':
		lcReturnValue = 'A'
	elif CountryName == 'CANADA':
		lcReturnValue = 'C'
	else:
		lcReturnValue = 'F'
	return lcReturnValue
$$ language plpythonu;

/*
Select udf_getlocation('usA');
Select udf_getlocation('canada');
Select udf_getlocation('Franc');
*/


/*  Reju Mathew  2021.01.25  Jira 626*/
create or replace function  udf_getscf(tczipfull varchar(20), tclocation varchar(max))
returns varchar(3)
stable
as $$
	lcreturnvalue =''
	if tczipfull is None:
		tczipfull =''
	if tclocation is None:
		tclocation =''
	tczipfull = tczipfull.strip().ljust(9, ' ')
	tclocation = tclocation.strip().upper()
	if tclocation == 'A':
		lcreturnvalue = tczipfull[:3]
	elif tclocation == 'C':
		lcreturnvalue = tczipfull[:3]
	else:
		lcreturnvalue =''
	return lcreturnvalue.strip().upper()
$$ language plpythonu;

/*
select udf_getscf('07624', 'A')
select udf_getscf('   0723', 'a')
*/

/*  Reju Mathew  2021.01.25  Jira 626*/
create or replace function udf_getdomain(cEmail varchar(max))
returns varchar(max)
stable
as $$
	cDomain=''
	if cEmail is not None:
		cEmail = cEmail.strip()
		sEmail = str(cEmail).split('@')
		if len(sEmail) > 0:
			cDomain = sEmail[-1]
		return cDomain.upper()
	else:
		return cDomain
$$ language plpythonu;


/*
select udf_getdomain('kellys@sdd.gmail.com');
select udf_getdomain('kellys@sdd.edu');
select udf_getdomain('kellys@sdd.gov');

*/

/*
  Reju Mathew  2021.01.25  Jira 626
 calling another UDF is not supported in redshift
*/
create or replace function udf_getdomaintype(cEmail varchar(max))
returns varchar(max)
stable
as $$
	def fn_getdomain(cEmail):
		cDomain=''
		if cEmail is not None:
			cEmail = cEmail.strip()
			sEmail = str(cEmail).split('@')
			if len(sEmail) > 0:
				cDomain = sEmail[-1]
			return cDomain
		else:
			return cDomain

	cDomain = fn_getdomain(cEmail).lower()
	cDomainType = ''
	defaultreturnvalue ='X'

 	if cDomain == '':
		return defaultreturnvalue

	if cDomain == 'aol.com':
		cDomainType = 'A'
	elif cDomain == 'yahoo.com':
		cDomainType = 'Y'
	elif cDomain == 'hotmail.com':
		cDomainType = 'H'

	if cDomainType =='':
		cDomainChars = cDomain.split('.')[-1]

		if cDomainChars == 'com': cDomainType='C'
		elif cDomainChars == 'mil': cDomainType='M'
		elif cDomainChars == 'edu': cDomainType = 'E'
		elif cDomainChars == 'gov': cDomainType = 'G'
		elif cDomainChars == 'net': cDomainType = 'N'
		elif cDomainChars == 'us': cDomainType = 'U'
		elif cDomainChars in ('ac', 'ad', 'ae', 'af', 'ag', 'ai', 'al', 'am', 'an', 'ao', 'aq', 'ar', 'as', 'at', 'au', 'aw', 'az',
						  'ba', 'bb', 'bd', 'be', 'bf', 'bg', 'bh', 'bi', 'bj', 'bm', 'bn', 'bo', 'br', 'bs', 'bt', 'bv', 'bw', 'by', 'bz',
						  'cc', 'cd', 'cf', 'cg', 'ch', 'ci', 'ck', 'cl', 'cm', 'cn', 'co', 'cr', 'cs', 'cu', 'cv', 'cx', 'cy', 'cz',
						  'de', 'dj', 'dk', 'dm', 'do', 'dz', 'ec', 'ee', 'eh', 'er', 'es', 'et', 'fi', 'fj', 'fk', 'fm','fo', 'fr',
						  'ga', 'gb', 'gd', 'ge', 'gf', 'gg', 'gh', 'gi', 'gm', 'gn', 'gp', 'gq', 'gr', 'gs', 'gt', 'gu', 'gw', 'gy',
						  'hk', 'hm', 'hn', 'hr', 'ht', 'hu', 'id', 'ie', 'il', 'im', 'in', 'int', 'io', 'iq', 'ir', 'is', 'it',
						  'je', 'jm', 'jo', 'jp', 'ke', 'kg', 'kh', 'ki', 'km', 'kn', 'kp', 'kr', 'kw', 'ky', 'kz',
						  'la', 'lb', 'lc', 'li', 'lk', 'lr', 'ls', 'lt', 'lu', 'lv', 'ly',
						  'ma', 'mc', 'md', 'mg', 'mh', 'mk', 'ml', 'mm', 'mn', 'mo', 'mp', 'mq', 'mr', 'ms', 'mt', 'mu', 'mv', 'mw', 'mx', 'my', 'mz',
						  'na', 'nc', 'ne', 'nf', 'ng', 'ni', 'nl', 'no', 'np', 'nr', 'nu', 'nz', 'om', 'pa', 'pe',
						  'pf', 'pg', 'ph', 'pk', 'pl', 'pm', 'pn', 'pr', 'ps', 'pt', 'pw', 'py',
						  'qa', 're', 'ro', 'ru', 'rw', 'sa', 'sb', 'sc', 'sd', 'de', 'sg', 'sh', 'si', 'sj', 'sk',
						  'sl', 'sm', 'sn', 'so', 'sr', 'st', 'su', 'sv', 'sy', 'sz',
						  'tc', 'td', 'tf', 'tg', 'th', 'tj', 'tk', 'tm', 'tn', 'to', 'tp', 'tr', 'tt', 'tv', 'tw', 'tz',
						  'ua', 'ug', 'uk', 'uy', 'uz', 'va', 'vc', 've', 'vg', 'vn', 'vu', 'wf', 'ws', 'ye', 'yt',
						  'yu', 'za', 'zm', 'zr', 'zw', 'vi'
						  ):
			cDomainType = 'F'
		elif cDomainChars == 'ca': cDomainType = 'D'
		elif cDomainChars == 'biz': cDomainType = 'B'
		elif cDomainChars in ('info', 'aero', 'arpa', 'aria', 'coop', 'jobs', 'mobi', 'museum', 'name', 'travel'):
			cDomainType = 'L'
		else:
			cDomainType = defaultreturnvalue
	return cDomainType
$$ language plpythonu;

/*
	--cDomain = udf_getdomain(cEmail).lower()
select udf_getdomaintype('reju_1_1mat@hotmail.com');
select udf_getdomaintype('sdfd@fdfd@gmail.zr');
select udf_getdomaintype('kellys@gmail.com');
select udf_getdomaintype('kellys@sdd.gov');
select udf_getdomaintype('kellys@sdd.mil');
select udf_getdomaintype('kellys@sdd.edu');
Select udf_GetdomainType('')

select * from svl_udf_log limit 50;

*/

/*  Reju Mathew  2021.01.25  Jira 626*/
create or replace function udf_hashstring (hashtype varchar(15), input_str varchar(500), caseInput varchar(20))
returns varchar(500)
stable
as $$
    import hashlib
    if not input_str  or not hashtype:
        return ''


    hashtype = hashtype.upper().strip()
    caseInput = caseInput.upper().strip()
    input_str = input_str.strip()


    if (caseInput =='LOWER' or caseInput ==''):
        input_str = input_str.lower()
    elif caseInput =='UPPER':
        input_str = input_str.upper()

    if input_str == '':
        return ''

    result = ''
    try:
        if hashtype == 'MD5':
            hashvalue = hashlib.md5(input_str.encode())
        elif hashtype == 'SHA256':
            hashvalue = hashlib.sha256(input_str.encode())
        elif hashtype == 'SHA512':
            hashvalue = hashlib.sha512(input_str.encode())

        result = hashvalue.hexdigest().lower()

    except:
        result = ''

    return result
$$ language plpythonu;

/*
 #No need of UPPER or lower based on input string. by default make it lower.  Calling program can set to upper, if needed
 #print (udf_hashstring('md5',None))  #2e94a9e0af37c231d03ec86c61a39cc4
    #print (udf_hashstring('sha256','   ','   '))  #2e94a9e0af37c231d03ec86c61a39cc4

Select udf_hashstring('md5','Rejumathew@infogroup.com','')  #2e94a9e0af37c231d03ec86c61a39cc4
Select udf_hashstring('md5','Rejumathew@infogroup.com','lower')  #2e94a9e0af37c231d03ec86c61a39cc4
Select UPPER(udf_hashstring('md5','Rejumathew@infogroup.com','lower'))  #2E94A9E0AF37C231D03EC86C61A39CC4
Select udf_hashstring('md5','Rejumathew@infogroup.com','Upper')  #f452718f7597f9b61526778c1183da20
    #
Select udf_hashstring('sha256','Rejumathew@infogroup.com','')  #b0bff3bc9ccfb53851618f2358d9d5adaa52ed593fa2eede6649f24ae41ac51e
Select udf_hashstring('sha256','Rejumathew@infogroup.com','lower')  #b0bff3bc9ccfb53851618f2358d9d5adaa52ed593fa2eede6649f24ae41ac51e
Select upper(udf_hashstring('sha256','Rejumathew@infogroup.com','lower'))  #B0BFF3BC9CCFB53851618F2358D9D5ADAA52ED593FA2EEDE6649F24AE41AC51E
Select udf_hashstring('sha256','Rejumathew@infogroup.com','Upper')  #da0b807b08432aec50a1afc1063a006a7a808250a38c15ac7dccd07347072c60

Select udf_hashstring('sha512','Rejumathew@infogroup.com','')      #ebc3612d8c62c09529402a7ae88a575b559761c3fd5654c1186c6610862e15bfb5f24bade0982ebeddc8c030d2a8573f78eebec453420e0fb4e67dc10fe46743
*/




/*  Reju Mathew  2021.01.25  Jira 626*/
create or replace function udf_FixEmail (tcemail varchar(500) )
returns varchar(100)
stable
as $$
    def udf_removeinvalidemailchars(tcemail):
      if not tcemail:
          return ''
      tcemail_new = ''
      for element in tcemail:
          if element in ['@', '.', '_', '#', '-', '+'] or element.isdigit() or element.isalpha():
                      tcemail_new = tcemail_new + element
      result = tcemail_new
      return result

      if not tcemail:
          return ''

    def udf_checkformatandfix(tcemail):

      if not tcemail:
          return ''

      tcemail_new = tcemail
      strextention = ''

      arrayofvaliddomainextention = ['.aero', '.arpa', '.asia', '.biz', '.cat', '.com', '.coop', '.edu', '.gov',
                                     '.info', '.int', '.jobs', '.mil', '.mobi', '.museum', '.name', '.net', '.org',
                                     '.pro', '.tel', '.travel']

      intlastampersand = -1
      intlastdot = -1
      find_all = lambda c, s: [x for x in range(c.find(s), len(c)) if c[x] == s]

      myarray = find_all(tcemail, '@')  # will return all positions of '@'
      # should have one @
      if len(myarray) != 1:
          return ''

      if len(myarray) > 0:
          intlastampersand = myarray[-1]  # get last element

      myarray = find_all(tcemail, '.')  # will return all positions of '.'
      if len(myarray) > 0:
          intlastdot = myarray[-1]  # get last element

      if intlastampersand > intlastdot:
          tcemail_new = tcemail_new + '.com'

      # If the last chars are not ".xx",  check for valid domain names. if invalid, add ".com".
      if tcemail_new[len(tcemail_new) - 3: len(tcemail_new) - 2] != '.':
          myarray = find_all(tcemail, '.')  # will return all positions of '.'
          if len(myarray) > 0:
              intlastdot = myarray[-1]  # get last element
          strextention = tcemail_new[intlastdot: len(tcemail_new)]
          strextention = strextention.lower()
          if strextention not in arrayofvaliddomainextention:
              if tcemail_new[len(tcemail_new) - 1: len(tcemail_new)] == '.':
                  tcemail_new = tcemail_new + 'com'
              else:
                  tcemail_new = tcemail_new + '.com'

      result = tcemail_new
      return result


    tcemail = tcemail.strip().lower()
    if tcemail.startswith(("www.", "http:", "@.", ".@")) or tcemail == '':
        return ''
    tcemail = tcemail.replace(",", ".")

    while '@@' in tcemail:
        tcemail = tcemail.replace('@@', '@')
    while '..' in tcemail:
        tcemail = tcemail.replace('..', '.')

    if tcemail.find('  ') > 0:
        tcemail = tcemail[0:tcemail.index(' ')]
    tcemail = tcemail.replace(" ", "")

    if tcemail.find('2') > 0 and tcemail.find('@') == -1:
        tcemail = tcemail.replace("2", "@")

    tcemail = udf_removeinvalidemailchars(tcemail)
    tcemail = udf_checkformatandfix(tcemail)

    if tcemail.startswith(('@', '.', '_', '-', '#', '+')):
        return ''
    if tcemail.find('@.') > 0 or tcemail.find('.@') > 0:
        return ''
    if tcemail.find('@') == -1 or tcemail.find('.') == -1:
        return ''

    result = tcemail.upper()
    return result
$$ language plpythonu;

/*
select udf_FixEmail('reju.#%123+=$^*mathew@data-axle.com')  --REJU.#123+MATHEW@DATA-AXLE.COM
select udf_FixEmail('reju.mathew@data-axle.com')  --REJU.MATHEW@DATA-AXLE.COM
select udf_FixEmail('a@b.com  c@d.com')   --A@B.COM
select udf_FixEmail('    ')  --''
select udf_FixEmail('    ')  --     ''
select udf_FixEmail('www.reju.mathew@data-axle.com')  --     ''
select udf_FixEmail('@.reju.mathew@data-axle.com') --     ''
select udf_FixEmail('.@.reju.mathew@data-axle.com') --     ''
select udf_FixEmail('reju@mathew.museum.') --    'REJU@MATHEW.MUSEUM.COM'
select udf_FixEmail('reju@mathew.jobs') --     'REJU@MATHEW.JOBS'
select udf_FixEmail('reju.mathew  @@@data-axle.com')  #Blank because of double blank     ''

*/


/*  Reju Mathew  2021.01.26  Jira 626. copied from transformer*/
create or replace function  udf_getstatecode(statename varchar(500), return_blank_if_not_found int)
returns varchar(500)
stable
as $$
    STATE_ABBREVIATION_BY_NAME = {
    'ALABAMA':                             'AL',
    'ALASKA':                              'AK',
    'ARIZONA':                             'AZ',
    'ARKANSAS':                            'AR',
    'CALIFORNIA':                          'CA',
    'COLORADO':                            'CO',
    'CONNECTICUT':                         'CT',
    'DELAWARE':                            'DE',
    'DISTRICTOFCOLUMBIA':                  'DC',
    'FLORIDA':                             'FL',
    'GEORGIA':                             'GA',
    'HAWAII':                              'HI',
    'IDAHO':                               'ID',
    'ILLINOIS':                            'IL',
    'INDIANA':                             'IN',
    'IOWA':                                'IA',
    'KANSAS':                              'KS',
    'KENTUCKY':                            'KY',
    'LOUISIANA':                           'LA',
    'MAINE':                               'ME',
    'MARYLAND':                            'MD',
    'MASSACHUSETTS':                       'MA',
    'MICHIGAN':                            'MI',
    'MINNESOTA':                           'MN',
    'MISSISSIPPI':                         'MS',
    'MISSOURI':                            'MO',
    'MONTANA':                             'MT',
    'NEBRASKA':                            'NE',
    'NEVADA':                              'NV',
    'NEWHAMPSHIRE':                        'NH',
    'NEWJERSEY':                           'NJ',
    'NEWMEXICO':                           'NM',
    'NEWYORK':                             'NY',
    'NORTHCAROLINA':                       'NC',
    'NORTHDAKOTA':                         'ND',
    'OHIO':                                'OH',
    'OKLAHOMA':                            'OK',
    'OREGON':                              'OR',
    'PENNSYLVANIA':                        'PA',
    'RHODEISLAND':                         'RI',
    'SOUTHCAROLINA':                       'SC',
    'SOUTHDAKOTA':                         'SD',
    'TENNESSEE':                           'TN',
    'TEXAS':                               'TX',
    'UTAH':                                'UT',
    'VERMONT':                             'VT',
    'VIRGINISLANDS':                       'VI',
    'VIRGINIA':                            'VA',
    'WASHINGTON':                          'WA',
    'WESTVIRGINIA':                        'WV',
    'WISCONSIN':                           'WI',
    'WYOMING':                             'WY',
    'ALBERTA':                             'AB',
    'NOVASCOTIA':                          'NS',
    'BRITISHCOLUMBIA':                     'BC',
    'ONTARIO':                             'ON',
    'MANITOBA':                            'MB',
    'PRINCEEDWARDISLAND':                  'PE',
    'NEWBRUNSWICK':                        'NB',
    'QUEBEC':                              'QC',
    'NEWFOUNDLAND':                        'NF',
    'SASKATCHEWAN':                        'SK',
    'NORTHWESTTERRITORIES':                'NT',
    'YUKONTERRITORY':                      'YT',
    'NUNAVUT':                             'NU',
    'AMERICANSAMOA':                       'AS',
    'GUAM':                                'GU',
    'MARSHALLISLANDS':                     'MH',
    'MICRONESIA':                          'FM',
    '(FEDERATEDSTATESOF)PALAU':            'PW',
    'PUERTORICO':                          'PR',
    'U.S.MINOROUTLYINGISLANDS':            'XX',
    'NORTHERNMARIANAISLANDS':              'MP',
    'ARMEDFORCESAFRICA':                   'AE',
    'ARMEDFORCESAMERICASAA(EXCEPTCANADA)': 'AA',
    'ARMEDFORCESCANADA':                   'AE',
    'ARMEDFORCESEUROPEAE':                 'AE',
    'ARMEDFORCESMIDDLEEASTAE':             'AE',
    'ARMEDFORCESPACIFICAP':                'AP',
    'FOREIGNOTHERNOTLISTEDABOVE':          'XX',
}

    def udf_calculatecountry(state_code):
        state_code = str(state_code).strip().upper()
        usa_state_1 = ['AL', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA',
                       'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA',
                       'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM',
                       'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
                       'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
        usa_state_2 = ['AK', 'HI']
        usa_state_3 = ['PR', 'QU', 'VI']
        usa_state_4 = ['AA', 'AE', 'AP', 'FP']
        canada_state = ['AB', 'BC', 'MB', 'NF', 'NS', 'NT', 'ON', 'PE', 'PQ', 'SK',
                        'YT', 'NB', 'NL', 'QC', 'NU']

        result = '6'
        if state_code in usa_state_1:
            result = '1'
        elif state_code in usa_state_2:
            result = '2'
        elif state_code in usa_state_3:
            result = '3'
        elif state_code in usa_state_4:
            result = '4'
        elif state_code in canada_state:
            result = '5'
        return result

    if not statename:
        return ''

    # remove spaces in between. NEW YORK becomes NEWYORK
    statename = statename.replace(' ', '').upper().strip()

    if statename in STATE_ABBREVIATION_BY_NAME:
        result = STATE_ABBREVIATION_BY_NAME[statename]
    elif udf_calculatecountry(statename) != '6': # US or Canada state abbreviation
        result = statename
    elif return_blank_if_not_found > 0:
        result = ''
    else:
        result = statename

    return result
$$ language plpythonu;

/*
Select udf_getstatecode('NEW YORK',1)  --    'NY'
Select udf_getstatecode('cali    fornia',1)  --    'CA'
Select udf_getstatecode('NE W YO RK',120)   --    'NY'
Select udf_getstatecode('NEWxYORK',0) ----    'NEWXYORK'
Select udf_getstatecode('NEWxYORK',1) --    ''
Select udf_getstatecode('PRINCEEDWARDISLAND',0) --    'PE'
Select udf_getstatecode('NLx',0) --    'NLX'
Select udf_getstatecode('AP',1)  ---    'AP'
Select udf_getstatecode('   ',0)--     ''
Select udf_getstatecode(Null,0)    --''
*/



create or replace function udf_isnumeric (inputvalue VARCHAR(max))
  returns bool
stable
as $$
    try:
        x = int(inputvalue)
    except:
        return (1==2)
    else:
        return (1==1)
$$ language plpythonu;


CREATE OR REPLACE FUNCTION f_is_date (x1 varchar)
RETURNS float
IMMUTABLE as $$
    import datetime
    try:
        datetime.datetime.strptime(x1, '%Y%m%d')
        return True
    except ValueError:
        return False
$$ LANGUAGE plpythonu;
