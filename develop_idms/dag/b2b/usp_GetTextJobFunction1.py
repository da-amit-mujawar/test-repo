import fnmatch


def usp_GetTextJobFunction1(ps_title):
    def find(ps_title_to_search, ps_wildcard):
        for rec in ps_wildcard:
            if ps_title_to_search.find(rec) >= 0:
                return True
        return False

    def re_find_list(ps_title_for_regex, ps_wildcard):
        for rec in ps_wildcard:
            if re_find(ps_title_for_regex, rec):
                return True
        return False

    def re_find(ps_title_for_regex, ps_wildcard):
        return fnmatch.fnmatch(ps_title_for_regex, ps_wildcard)

    def chk_in(ps_value, pl_list):
        if ps_value in pl_list:
            return True
        else:
            return False

    ret_value = ""

    ls_title = str(ps_title).strip().lower()
    ls_title_orignal = ls_title
    la_invalid_chars = [(',', ''), ('#', ''), ("'", ''), ('.', ''), ('-', ''), ('/', ' '),
                        ('\\', ' '), ('v p ', 'vp '), ('c e o ', 'ceo ')]
    for x in la_invalid_chars:
        ls_title = ls_title.replace(x[0], x[1])
    ls_title = ls_title.strip()
    ls_title_no_space = str(ls_title.replace(' ', ''))
    la_null_values = ['programdirector', 'minimumdatasetcoordinator', 'federalprogramdirector']
    if ls_title_no_space == 'directorofsocialservices':
        ret_value = 'e03'
    elif ls_title_no_space in ['audiologist', 'directorofaudiology', 'dr']:
        ret_value = 'e16'
    elif ls_title_no_space == 'puragt':
        ret_value = 'm02'
    elif ls_title_no_space == 'cntrllr':
        ret_value = 'a13'
    elif ls_title_no_space in ['paraprofessionalinstructaide', 'specialneedsaide']:
        ret_value = 'r10'
    elif ls_title_no_space == 'dochiropractor':
        ret_value = 'e42'
    elif ls_title_no_space == 'internetdomainadmin':
        ret_value = 'v08'

    elif ls_title in ['coo', 'c o o']:
        ret_value = '632'
    elif ls_title == 'lab':
        ret_value = 'w18'
    elif ls_title == 'pharm d':
        ret_value = 'e55'
    elif ls_title == 'pharmacist':
        ret_value = 'e55'
    elif ls_title in ['assistant principal', 'assistant or vice principa', 'prin', 'vice principal',
                      'associate principal', 'elementary principal']:
        ret_value = '660'
    elif ls_title in ['senior principal', 'middle/junior high principal', 'asst principal',
                      'schl prin',
                      'sr principal', 'prinicipal']:
        ret_value = '660'
    elif ls_title in ['executive secretary', 'administrative secretary', 'admin secretary']:
        ret_value = '664'
    elif ls_title == 'ot':
        ret_value = 'e53'
    elif chk_in(ls_title, ['dirops', 'mgrops', 'vpops', 'sr vpops', 'exec vpops']):
        ret_value = '632'
    elif chk_in(ls_title, ['program director', 'programdirector']):
        ret_value = '672'
    elif chk_in(ls_title, ['pres cfo', 'vp  cfo']):
        ret_value = 'a31'
    elif chk_in(ls_title, ['physical therapist']):
        ret_value = 'e56'
    elif chk_in(ls_title, ['vpsls', 'mgrsls', 'vp sls', 'dirsls', 'dir sls', 'sls rep', 'sls',
                           'sls manager', 'sr vpsls', 'director sls', 'sr vp sls']):
        ret_value = 'h05'
    elif chk_in(ls_title, ['pur mgr']):
        ret_value = 'm02'
    elif ls_title_no_space in la_null_values:
        ret_value = ''
    elif find(ls_title, ['b30']):
        ret_value = '306'
    elif find(ls_title, ['plt. mgr.', 'plt mgr']):
        ret_value = '401'
    elif find(ls_title, ['religious leader']):
        ret_value = '610'
    elif find(ls_title, ['vp-ops', 'coo & exec vp', 'pres., coo', 'coo & vp', 'coo & sr vp']):
        ret_value = '632'
    elif find(ls_title, ['evp and coo', 'vp coo', 'v-p., coo', 'ceo/coo', 'ex. v-p., coo']):
        ret_value = '632'
    elif find(ls_title, ['president / coo', 'coo - coo', 'vp/coo', 'coo & cfo', 'evp/coo',
                         'co-founder & coo']):
        ret_value = '632'
    elif find(ls_title, ['materials manager']):
        ret_value = '671'
    elif find(ls_title, ['program director', 'director program management', 'program chair',
                         'director of geriatric programs']):
        ret_value = '672'
    elif find(ls_title, ['corp. secy.', 'corp sec', 'president secretary', 'president; secretary']):
        ret_value = 'a14'

    elif (find(ls_title, ['vp-fin', 'pres., cfo', 'dir-fin', 'mgr-fin', 'pres & cfo', 'sr vp-fin',
                          'pres, ceo & cfo'])) and (ls_title[-3:]) in ('cfo', 'fin'):
        ret_value = 'a31'

    elif (find(ls_title,
               ['ceo & cfo', 'fin', 'interim cfo', 'vp /cfo', 'chm, pres, ceo & cfo', 'exec vp-fin',
                'ceo cfo',
                'executive vice president cfo'])) and (ls_title[-3:]) in ('cfo', 'fin'):
        ret_value = 'a31'
    elif find(ls_title, ['accounts manager']):
        ret_value = 'a37'
    elif find(ls_title,
              ['dir/mgr/dept head/chief/supt - personnel', 'student personnel director',
               'chief of personnel',
               'personneldirector', 'dir-personnel']):
        ret_value = 'b11'
    elif find(ls_title, ['pharmacy manager', 'director of pharmacy', 'pharmacy director',
                         'pharmacydirector',
                         'director pharmacy']):
        ret_value = 'e55'
    elif find(ls_title, ['director of physical therapy']):
        ret_value = 'e56'
    elif find(ls_title, ['vp-sls', 'mgr-sls', 'sls mgr']):
        ret_value = 'h05'
    elif find(ls_title, ['marketing programs manager', 'marketing program manager']):
        ret_value = 'i01'
    elif find(ls_title, ['account director']):
        ret_value = 'i02'
    elif find(ls_title, ['pur. mgr.']):
        ret_value = 'm02'
    elif find(ls_title, ['cafeteria manager']):
        ret_value = 't02'
    elif find(ls_title, ['corp comms']):
        ret_value = 'u01'
    elif find(ls_title, ['information technology services', 'information technology service',
                         'information technology program manager',
                         'information technology auditor',
                         'information technology services manager']):
        ret_value = 'v09'
    elif find(ls_title, ['information technology services', 'information technology service',
                         'information technology program manager',
                         'information technology auditor',
                         'information technology services manager']):
        ret_value = 'v09'
    elif find(ls_title,
              ['director information technology services', 'it program manager', 'it auditor',
               'director of information technology services', 'it audit manager',
               'manager information technology services']):
        ret_value = 'v09'
    elif find(ls_title, ['it services manager', 'information technology service manager',
                         'vice president information technology services',
                         'information technology technology services',
                         'director - information technology services']):
        ret_value = 'v09'
    elif find(ls_title, ['web project manager', 'web manager', 'website manager',
                         'web content manager',
                         'web coordinator', 'website coordinator', 'web site manager',
                         'web director',
                         'web multimedia developer', 'website administrator',
                         'website developer']):
        ret_value = 'v24'
    elif find(ls_title, ['lan administrator']):
        ret_value = 'v27'
    elif find(ls_title, ['product/system design engineering', 'system design engineer']):
        ret_value = 'v29'
    elif find(ls_title, ['research associate', 'research manager', 'research assistant',
                         'director of research',
                         'research director', 'research fellow', 'senior researcher',
                         'senior research associate', 'research assoc', 'research officer',
                         'market research manager']):
        ret_value = 'w17'
    elif find(ls_title, ['research coordinator', 'vp research', 'sr research associate',
                         'market research',
                         'vice president research', 'director research',
                         'senior research fellow',
                         'research development', 'principal researcher',
                         'research development manager',
                         'research asst']):
        ret_value = 'w17'
    elif find(ls_title, ['vice president of research', 'clinical research manager',
                         'senior research assoc',
                         'director market research', 'clinical research associate',
                         'manager market research',
                         'research vice president', 'sr researcher',
                         'clinical research coordinator',
                         'senior research manager', 'director, research']):
        ret_value = 'w17'

    elif find(ls_title,
              ['laboratory manager', 'laboratory director', 'director of laboratory services',
               'laboratory technician', 'director of laboratory',
               'director of laboratory servi',
               'laboratory supervisor', 'laboratorydirector',
               'manager of laboratory services',
               'laboratory technologist', 'laboratory specialist',
               'manager of laboratory servic']):
        ret_value = 'w18'
    elif find(ls_title,
              ['dir medical records', 'dir of medical records', 'director medical records',
               'director of medical records']):
        ret_value = '507'
    elif find(ls_title,
              ['manager of medical records', 'medical records', 'medical records clerk',
               'medical records coordinator']):
        ret_value = '507'
    elif find(ls_title, ['medical records director', 'medical records manager',
                         'medical records technician',
                         'vice president of medical records']):
        ret_value = '507'
    elif find(ls_title, ['program coord']):
        ret_value = '672'
    elif find(ls_title, ['religious dir', 'religious ldr']):
        ret_value = '610'
    elif find(ls_title,
              ['c o o', 'chief operating officer', 'chief operating offr', 'coo/exec vp']):
        ret_value = '632'
    elif find(ls_title, ['exec vp & chief operating offi',
                         'executive vice president and chief operating offic',
                         'pres & coo', 'president & chief operating officer']):
        ret_value = '632'
    elif find(ls_title, ['president & coo', 'president and chief operating officer',
                         'president and coo',
                         'president chief operating officer']):
        ret_value = '632'
    elif find(ls_title, ['president coo', 'president/coo',
                         'senior vice president and chief operating officer',
                         'vice president and chief operating officer', 'vp & coo']):
        ret_value = '632'
    elif find(ls_title,
              ['assistant program manager', 'deputy program manager', 'director of programs',
               'evening mba program']) and (
            (not find(ls_title, ['programer'])) and (not find(ls_title, ['programmer'])) and (
            not find(ls_title, ['programming']))):
        ret_value = '672'
    elif find(ls_title,
              ['federal programs director', 'program administrator', 'program assistant',
               'program associate']) and (
            (not find(ls_title, ['programer'])) and (not find(ls_title, ['programmer'])) and (
            not find(ls_title, ['programming']))):
        ret_value = '672'
    elif find(ls_title,
              ['program coord', 'program coordinator', 'program developer', 'program dir',
               'program director']) and (
            (not find(ls_title, ['programer'])) and (not find(ls_title, ['programmer'])) and (
            not find(ls_title, ['programming']))):
        ret_value = '672'
    elif find(ls_title,
              ['program manager', 'program mgr', 'program officer', 'program supervisor',
               'program support assistant']) and (
            (not find(ls_title, ['programer'])) and (not find(ls_title, ['programmer'])) and (
            not find(ls_title, ['programming']))):
        ret_value = '672'
    elif find(ls_title,
              ['programdirector', 'programer', 'programme manager', 'programs director',
               'programs manager']) and (
            (not find(ls_title, ['programer'])) and (not find(ls_title, ['programmer'])) and (
            not find(ls_title, ['programming']))):
        ret_value = '672'
    elif find(ls_title,
              ['senior program director', 'senior program manager', 'senior program officer',
               'sr program manager']) and (
            (not find(ls_title, ['programer'])) and (not find(ls_title, ['programmer'])) and (
            not find(ls_title, ['programming']))):
        ret_value = '672'

    elif find(ls_title, ['dir-fin', 'evp & cfo', 'evp and cfo', 'evp/cfo', 'mgr-fin',
                         'senior vp & cfo']):
        ret_value = 'a31'
    elif find(ls_title,
              ['svp and cfo', 'svp/cfo', 'vp & cfo', 'vp and cfo', 'vp cfo', 'vp/cfo',
               'vp-fin']):
        ret_value = 'a31'
    elif find(ls_title,
              ['dir/mgr/dept head/chief/supt - personnel', 'director of personnel',
               'mgr personnel dept',
               'vp personnel']):
        ret_value = 'b11'
    elif find(ls_title, ['chairman of pediatric services', 'chief of pediatric services',
                         'chief of pediatrics',
                         'critical care - pediatric']):
        ret_value = 'e28'
    elif find(ls_title, ['director of pediatric services', 'ophthalmology/pediatrics',
                         'pediatric cardiology',
                         'pediatric emergency medicine']):
        ret_value = 'e28'
    elif find(ls_title,
              ['pediatric emergency medicine-em', 'pediatric endocrinologist',
               'pediatric endocrinology',
               'pediatric ophthalmologist']):
        ret_value = 'e28'
    elif find(ls_title,
              ['pediatric pulmonologist', 'pediatric surgery', 'pediatric urologist',
               'pediatrics']):
        ret_value = 'e28'
    elif find(ls_title, ['dir occupational therapy', 'occupational medicine',
                         'occupational medicine spec',
                         'occupational therapist', 'occupational therapy director']):
        ret_value = 'e53'
    elif find(ls_title,
              ['chief pharmacist', 'clinical pharmacist', 'clinical staff pharmacist',
               'consultant pharmacist',
               'evening staff pharmacist']) and (
            (not find(ls_title, ['pharmaceutical'])) or (
            not find(ls_title, ['biopharmaceutical'])) or (
                    not find(ls_title, ['pharmacologist']))):
        ret_value = 'e55'

    elif find(ls_title,
              ['lead pharmacist', 'owner chief pharmacist', 'owner pharmacist',
               'owner/chief pharmacist',
               'owner/pharmacist']) and (
            (not find(ls_title, ['pharmaceutical'])) or (
            not find(ls_title, ['biopharmaceutical'])) or (
                    not find(ls_title, ['pharmacologist']))):
        ret_value = 'e55'

    elif find(ls_title,
              ['ownerchief pharmacist', 'ownerpharmacist', 'pharmacist in charge',
               'pharmacist manager',
               'pharmacist owner', 'pharmacist(clinical spec)', 'registered pharmacist',
               'relief pharmacist',
               'staff pharmacist']) and (
            (not find(ls_title, ['pharmaceutical'])) or (
            not find(ls_title, ['biopharmaceutical'])) or (
                    not find(ls_title, ['pharmacologist']))):
        ret_value = 'e55'

    elif find(ls_title, ['director of rehab', 'director of rehab services',
                         'director of rehabilitation',
                         'director of rehabilitation svs']) and (
            (not find(ls_title, ['sales'])) or (not find(ls_title, ['const'])) or (
            not find(ls_title, ['concrete'])) or (not find(ls_title, ['build'])) or (
                    not find(ls_title, ['bldg'])) or (
                    not find(ls_title, ['eq'])) or (not find(ls_title, ['recruiter'])) or (
                    not find(ls_title, ['util']))):
        ret_value = 'e56'
    elif find(ls_title, ['physical medicine & rehab', 'physical medicine/rehab spec',
                         'physical rehab medicine',
                         'rehab director']) and (
            (not find(ls_title, ['sales'])) or (not find(ls_title, ['const'])) or (
            not find(ls_title, ['concrete'])) or (not find(ls_title, ['build'])) or (
                    not find(ls_title, ['bldg'])) or (
                    not find(ls_title, ['eq'])) or (not find(ls_title, ['recruiter'])) or (
                    not find(ls_title, ['util']))):
        ret_value = 'e56'
    elif find(ls_title,
              ['rehab manager', 'rehab specialist', 'rehab tech',
               'sports med (physical & rehab)']) and (
            (not find(ls_title, ['sales'])) or (not find(ls_title, ['const'])) or (
            not find(ls_title, ['concrete'])) or (not find(ls_title, ['build'])) or (
                    not find(ls_title, ['bldg'])) or (
                    not find(ls_title, ['eq'])) or (not find(ls_title, ['recruiter'])) or (
                    not find(ls_title, ['util']))):
        ret_value = 'e56'

    elif find(ls_title, ['bus dev', 'bus dev manager', 'bus dev mgr', 'business dev',
                         'business dev manager']):
        ret_value = 'h07'
    elif find(ls_title,
              ['business dev mgr', 'business dev. mgr', 'business development associate',
               'business development coordinator']):
        ret_value = 'h07'
    elif find(ls_title,
              ['business development dir', 'business development directo',
               'business development director',
               'business development executive']):
        ret_value = 'h07'
    elif find(ls_title, ['business development manager', 'business development mgr',
                         'business development rep',
                         'business development representative']):
        ret_value = 'h07'
    elif find(ls_title,
              ['business development vice president', 'business developmentdirector',
               'business devt manager',
               'dir - business devel']):
        ret_value = 'h07'
    elif find(ls_title, ['dir bus dev', 'dir business development', 'dir of business devel',
                         'dir of business development', 'dir-bus dev']):
        ret_value = 'h07'

    elif find(ls_title,
              ['director - business development', 'director bus dev', 'director business dev',
               'director business develop', 'director business developm']):
        ret_value = 'h07'
    elif find(ls_title,
              ['director business developmen', 'director business development',
               'director of business dev',
               'director of business devel', 'director of business develop']):
        ret_value = 'h07'
    elif find(ls_title, ['director of business developme', 'director of business developmen',
                         'director of business development', 'director-business development']):
        ret_value = 'h07'
    elif find(ls_title,
              ['executive vice president business development', 'head of business development',
               'manager business development', 'manager of business development']):
        ret_value = 'h07'
    elif find(ls_title,
              ['manager-business development', 'mgr-bus dev', 'new business development',
               'new business development manager', 'senior business development manager']):
        ret_value = 'h07'
    elif find(ls_title,
              ['senior director business development', 'senior manager business development',
               'senior vice president business development',
               'senior vice president of business development']):
        ret_value = 'h07'
    elif find(ls_title,
              ['svp business development', 'vice president - business development',
               'vice president bus dev',
               'vice president business development', 'vice president business development']):
        ret_value = 'h07'
    elif find(ls_title, ['vice president business dvmt', 'vice president of business dev',
                         'vice president of business development',
                         'vice president,  business development']):
        ret_value = 'h07'
    elif find(ls_title,
              ['vice president-business development', 'vice-president, business development',
               'vp - business development', 'vp bus dev', 'vp business development']):
        ret_value = 'h07'
    elif find(ls_title,
              ['vp of business development', 'vp, business development', 'vp-bus dev']):
        ret_value = 'h07'
    elif find(ls_title, ['product/system design engineering']):
        ret_value = 'n09'
    elif find(ls_title, ['curriculum developer']):
        ret_value = 'r13'
    elif find(ls_title, ['dental hygiene student', 'dental student', 'doctoral student',
                         'medical student']):
        ret_value = 'r16'
    elif find(ls_title, ['information controller']):
        ret_value = 'v09'
    elif find(ls_title, ['system director']):
        ret_value = 'v26'
    elif find(ls_title, ['programer', 'system designer', 'system developer']):
        ret_value = 'v29'
    elif find(ls_title,
              ['director health information management', 'director him',
               'director of health information',
               'director of him', 'health information management',
               'health information manager', 'him director',
               'him manager']):
        ret_value = 'v32'
    elif find(ls_title,
              ['cardiac cath lab director', 'cardiac cath labdirector',
               'director of catheterization lab',
               'director of laboratory', 'director of laboratory servi',
               'director of laboratory services']) and (
            (not find(ls_title, ['computer'])) and (not find(ls_title, ['labor']))):
        ret_value = 'w18'

    elif find(ls_title,
              ['lab', 'lab analyst', 'lab assistant', 'lab coordinator', 'lab dir',
               'lab dir chief scienti', 'lab director', 'lab director manager']) and (
            (not find(ls_title, ['computer'])) and (not find(ls_title, ['labor']))):
        ret_value = 'w18'

    elif find(ls_title, ['lab director/lab manager', 'lab director/manager',
                         'lab director/manager/supervisor',
                         'lab management', 'lab manager', 'lab mgr', 'lab mgr.']) and (
            (not find(ls_title, ['computer'])) and (not find(ls_title, ['labor']))):
        ret_value = 'w18'
    elif find(ls_title,
              ['lab supervisor', 'lab supervisor / manager /', 'lab supervisor manager',
               'lab supv',
               'lab tech', 'lab technician', 'laboratory director']) and (
            (not find(ls_title, ['computer'])) and (not find(ls_title, ['labor']))):
        ret_value = 'w18'
    elif find(ls_title,
              ['laboratory manager', 'laboratory specialist', 'laboratory supervisor',
               'laboratory technician',
               'laboratory technologist', 'laboratorydirector',
               'manager of laboratory servic']) and (
            (not find(ls_title, ['computer'])) and (not find(ls_title, ['labor']))):
        ret_value = 'w18'
    elif find(ls_title, ['manager of laboratory services', 'senior lab technician']) and (
            (not find(ls_title, ['computer'])) and (not find(ls_title, ['labor']))):
        ret_value = 'w18'
    elif find(ls_title, ['interim director']):
        ret_value = 'z9z'
    elif re_find_list(ls_title, ['*cert* pub* acc*', 'cpa *', '* cpa']) or find(ls_title,
                                                                                [' cpa ']) or (
            ls_title == 'cpa'):
        ret_value = 'a11'
    elif re_find_list(ls_title, ['fin *', '* fin', '*chief* fin* off*', '*chief* fin* off*',
                                 'cfo *']) or find(ls_title,
                                                   [' fin ', 'financ', ' cfo ']) or chk_in(ls_title,
                                                                                           ['cfo']):
        ret_value = 'a31'
    elif find(ls_title, ['budget']):
        ret_value = 'a32'
    elif re_find_list(ls_title, ['*econ* forecast*']) or find(ls_title, ['demand forecast']):
        ret_value = 'a33'
    elif find(ls_title, ['revenue']):
        ret_value = 'a34'
    elif re_find_list(ls_title, ['*acct* pay*']) or find(ls_title, ['accounts pay']):
        ret_value = 'a28'
    elif re_find_list(ls_title, ['*acct* receiv*']) or find(ls_title, ['accounts receiv']):
        ret_value = 'a27'
    elif find(ls_title, ['cost account', 'cost acct']):
        ret_value = 'a29'
    elif find(ls_title, ['credit']):
        ret_value = 'a30'
    elif find(ls_title, ['stock']):
        ret_value = 'a20'
    elif find(ls_title, ['bond ', ' bond ']) or re_find_list(ls_title, ['* bonds']):
        ret_value = 'a21'
    elif find(ls_title, ['trusts']):
        ret_value = 'a25'
    elif find(ls_title, ['merger', 'acquisition']):
        ret_value = 'a18'
    elif find(ls_title, ['invest']):
        ret_value = 'a17'
    elif find(ls_title, ['banker']):
        ret_value = 'a08'
    elif find(ls_title, ['bank']):
        ret_value = 'a09'
    elif find(ls_title, ['auditor']):
        ret_value = 'a07'
    elif find(ls_title, ['audit']):
        ret_value = 'a06'
    elif find(ls_title, ['securities']) and (
            (not find(ls_title, ['comp'])) and (not find(ls_title, ['net'])) and (
            not find(ls_title, ['sys']))):
        ret_value = 'a19'
    elif find(ls_title, ['tax']):
        ret_value = 'a22'
    # 07/07 - Ram - Added Not condition to match SQL Query
    #     WHEN @M_TEXT_TITLE LIKE '%TELLER%' AND  @M_TEXT_TITLE NOT LIKE '%FORTUNE%'  THEN 'A23'
    elif find(ls_title, ['teller']) and (not find(ls_title, ['fortune'])):
        ret_value = 'a23'
    elif find(ls_title, ['econom']):
        ret_value = 'a24'
    elif find(ls_title, ['treas']) and not find(ls_title, ['treasury']):
        ret_value = 'a25'
    elif find(ls_title, ['accountant']):
        ret_value = 'a01'
    elif find(ls_title, ['accounting', 'accting']):
        ret_value = 'a26'
    elif find(ls_title, ['adjuster']):
        ret_value = 'a02'
    elif find(ls_title, ['apprais']):
        ret_value = 'a03'
    elif find(ls_title, ['assessor']):
        ret_value = 'a04'
    elif find(ls_title, ['bond']) and not find(ls_title, ['bail']):
        ret_value = 'a10'
    elif find(ls_title, ['collector', 'collections']):
        ret_value = 'a12'
    elif find(ls_title, ['controller', 'comptroller', 'ctrlr', 'cntrlr']):
        ret_value = 'a13'
    elif find(ls_title, ['corp']) and find(ls_title, ['secretary']):
        ret_value = 'a14'
    elif find(ls_title, ['estimator']):
        ret_value = 'a35'
    elif find(ls_title, ['employement', 'recruit']):
        ret_value = 'b01'
    elif find(ls_title, ['affirmative action']):
        ret_value = 'b02'
    elif find(ls_title, ['wage admin', 'salary']):
        ret_value = 'b03'
    elif re_find_list(ls_title, ['*emp* benefit*']) or find(ls_title, ['benefits']):
        ret_value = 'b04'
    elif find(ls_title, ['personnel relation']):
        ret_value = 'b05'
    elif find(ls_title, ['labor relation']):
        ret_value = 'b06'
    elif find(ls_title, ['pension']):
        ret_value = 'b07'
    elif find(ls_title, ['multicultural affairs', 'diversity']):
        ret_value = 'b08'
    elif find(ls_title, ['disability']):
        ret_value = 'b09'
    elif find(ls_title, ['osha', 'safety']):
        ret_value = 'b10'
    elif re_find_list(ls_title, ['personnel *']) or fnmatch.fnmatch(ls_title, 'personnel'):
        ret_value = 'b11'
    elif find(ls_title, ['hr generalist']) or re_find_list(ls_title, ['*human res* generalist*']):
        ret_value = 'b12'
    elif find(ls_title, ['payroll']):
        ret_value = 'b13'
    elif find(ls_title, ['compensation']):
        ret_value = 'b14'
    elif find(ls_title, ['teacher']) or (ls_title == 'tchr') or (
            re_find_list(ls_title, ['* tchr', 'tchr *'])):
        ret_value = 'r08'
    # 07/07 - Ram - Rewrite the condition
    #     WHEN @M_TEXT_TITLE LIKE '% HR %'
    # 		OR @M_TEXT_TITLE LIKE '%HUMAN RESOURCE%'
    # 		OR @M_TEXT_TITLE LIKE 'HR %'
    # 		OR @M_TEXT_TITLE LIKE '% HR'
    # 		OR @M_TEXT_TITLE_Orginal LIKE '%-HR'
    # 		OR @M_TEXT_TITLE_Orginal LIKE 'HUM% RES%'
    # 		OR @M_TEXT_TITLE_Orginal LIKE '% HUM.RES.'
    # 		OR @M_TEXT_TITLE_Orginal LIKE '% HUM% RES%'
    # 		OR @M_TEXT_TITLE IN ('HR','HUMAN RESOURCE','HRMANAGER','HR MANAGER')
    # 		THEN 'B15'
    elif find(ls_title, [' hr ', 'human resource']) \
            or re_find_list(ls_title,
                            ['hr *', '* hr']) \
            or chk_in(ls_title, ['hr', 'human resource', 'hrmanager', 'hr manager']) or \
            re_find_list(ls_title_orignal, ['*-hr', 'hum* res*', '* hum.res.', '* hum* res*']):
        ret_value = 'b15'
    elif find(ls_title, ['book publish']):
        ret_value = 'c06'
    elif find(ls_title, ['newspaper publish']):
        ret_value = 'c07'
    elif find(ls_title, ['magazine publish']):
        ret_value = 'c08'
    elif find(ls_title, ['publisher']):
        ret_value = 'c01'
    elif find(ls_title, ['editor', 'editing']):
        ret_value = 'c02'
    elif find(ls_title, ['reporter']):
        ret_value = 'c03'
    elif find(ls_title, ['writer']):
        ret_value = 'c04'
    elif find(ls_title, ['illustrator']):
        ret_value = 'c05'
    elif find(ls_title, ['publish']):
        ret_value = 'c09'
    elif re_find(ls_title, '*heavy build* construction*'):
        ret_value = 'd05'
    elif re_find(ls_title, '*light build* construction*'):
        ret_value = 'd06'
    elif re_find(ls_title, '*commercial build* construction*'):
        ret_value = 'd01'
    elif re_find(ls_title, '*resid* build* construction*'):
        ret_value = 'd08'
    elif find(ls_title, ['remodel']):
        ret_value = 'd07'
    elif find(ls_title, ['contractor']):
        ret_value = 'd03'
    elif find(ls_title, ['construction', 'carpenter']):
        ret_value = 'd02'
    elif find(ls_title, ['allerg']) and not find(ls_title, ['metal']):
        ret_value = 'e01'
    elif find(ls_title, ['anesthesiolog']):
        ret_value = 'e02'
    elif find(ls_title, ['social work']):
        ret_value = 'e03'
    elif find(ls_title, ['case worker']):
        ret_value = 'e03'
    elif find(ls_title, ['dentist']) or fnmatch.fnmatch(ls_title, 'dds'):
        ret_value = 'e04'
    elif find(ls_title, ['dermatolog']):
        ret_value = 'e05'
    elif find(ls_title, ['endodont']):
        ret_value = 'e06'
    elif find(ls_title, ['family practitioner', 'family practice', 'do - family medicine']):
        ret_value = 'e07'
    elif find(ls_title, ['gastroenterolog']):
        ret_value = 'e08'
    elif find(ls_title, ['general practitioner', 'general practice']):
        ret_value = 'e09'
    elif find(ls_title, ['gerontolog']):
        ret_value = 'e10'
    elif find(ls_title, ['gynecolog']):
        ret_value = 'e11'
    elif find(ls_title, ['head and neck']):
        ret_value = 'e12'
    elif find(ls_title, ['hematolog']):
        ret_value = 'e13'
    elif find(ls_title, ['internist', 'internal medicine', 'md - internal medicine',
                         'do - internal medicine']):
        ret_value = 'e14'
    elif find(ls_title, ['laryngolog']):
        ret_value = 'e15'
    elif find(ls_title, ['nephrolog']):
        ret_value = 'e17'
    elif find(ls_title, ['neurolog']):
        ret_value = 'e18'
    elif find(ls_title, ['nurse', 'nursing', ' rn ', 'lpn']) or fnmatch.fnmatch(ls_title,
                                                                                'rn') or \
            re_find_list(ls_title, ['rn *', '* rn']):
        ret_value = 'e19'
    elif find(ls_title, ['obstetrician']):
        ret_value = 'e20'
    elif find(ls_title, ['oral surg']):
        ret_value = 'e21'
    elif find(ls_title, ['orthodontist']):
        ret_value = 'e22'
    elif find(ls_title, ['orthopedist', 'orthopedics']):
        ret_value = 'e23'
    elif find(ls_title, ['osteopath']):
        ret_value = 'e24'
    elif find(ls_title, ['otologist']):
        ret_value = 'e25'
    elif find(ls_title, ['otorhinolaryngolog']):
        ret_value = 'e26'
    elif find(ls_title, ['patholog']):
        ret_value = 'e27'
    elif find(ls_title, ['pediatrician']):
        ret_value = 'e28'
    elif find(ls_title, ['periodontist']):
        ret_value = 'e29'
    elif find(ls_title, ['physician', 'physican']):
        ret_value = 'e30'
    elif find(ls_title, ['psychiatrist', 'psychology professional',
                         'psychiatry', 'psychology', 'do - psychiatry orthomolecular']):
        ret_value = 'e31'
    elif find(ls_title, ['psychologist']):
        ret_value = 'e31'
    elif find(ls_title, ['radiolog']):
        ret_value = 'e32'
    elif find(ls_title, ['surgeon']):
        ret_value = 'e33'
    elif re_find(ls_title, '*urolog'):
        ret_value = 'e34'
    elif find(ls_title, ['veterinarian', 'dvm']):
        ret_value = 'e35'
    elif find(ls_title, ['private health care']):
        ret_value = 'e36'
    elif find(ls_title, ['public health care']):
        ret_value = 'e37'
    elif find(ls_title, ['health depart']):
        ret_value = 'e38'
    elif find(ls_title, ['hospital']) and not find(ls_title, ['hospitality']):
        ret_value = 'e39'
    elif find(ls_title,
              ['medical', 'health', 'therapist', 'psychiatrist', 'physician', 'nurse', 'nursing',
               'veterinar', 'health',
               'dental', 'dentist', 'pharmacist', 'pediatric', 'sugeon', 'rehab', 'chriopract',
               'surgery', 'doctor',
               'm d', 'doctors', 'doctor of medicine', 'pharmacologist', 'pulmonologist',
               'rheumatologist',
               'occupational medicine spec', 'pulmonary critical care medicine', 'dom',
               'epidemiologist',
               'doctor - medicine', 'rheumatology', 'critical care medicine', 'neonatology',
               'exercise physiologist',
               'perinatologist', 'neonatologist', 'embryologist', 'md - rheumatology']) or chk_in(
        ls_title,
        ['rn', 'md', 'dmd',
         'dds']) or re_find(
            ls_title, 'med *') or ls_title == 'do':
        ret_value = 'e16'
    elif find(ls_title, ['local gov', 'mayor']):
        ret_value = 'f22'
    elif find(ls_title, ['military']):
        ret_value = 'f26'
    elif find(ls_title, ['army']):
        ret_value = 'f05'
    elif find(ls_title, ['navy']):
        ret_value = 'f28'
    elif find(ls_title, ['air force']):
        ret_value = 'f03'
    elif find(ls_title, ['us marine']):
        ret_value = 'f24'
    elif re_find_list(ls_title, ['*def* dep*', '*dep* of def*']) or fnmatch.fnmatch(ls_title,
                                                                                    'dod'):
        ret_value = 'f11'
    elif re_find(ls_title, '*dep* of labor*') or find(ls_title, ['labor dep']):
        ret_value = 'f20'
    elif find(ls_title, ['state dep']):
        ret_value = 'f35'
    elif re_find_list(ls_title, ['*agricu* dep*', '*dep* of agr*']):
        ret_value = 'f02'
    elif find(ls_title, ['justice dep']):
        ret_value = 'f19'
    elif find(ls_title, ['admiral']):
        ret_value = 'f01'

    elif find(ls_title, ['capt']) and (
            (not find(ls_title, ['job'])) and (not find(ls_title, ['line'])) and (
            not find(ls_title, ['training']))):
        ret_value = 'f06'
    elif find(ls_title, ['colonel']):
        ret_value = 'f07'
    elif find(ls_title, ['commander']):
        ret_value = 'f08'
    elif re_find_list(ls_title, ['*commerce* dep*', '*dep* of commerce*']):
        ret_value = 'f09'
    elif find(ls_title, ['corporal']):
        ret_value = 'f10'
    elif find(ls_title, ['ensign']):
        ret_value = 'f13'
    elif find(ls_title, ['state gov']):
        ret_value = 'f36'
    elif re_find(ls_title, '*fed* gov*'):
        ret_value = 'f14'
    elif find(ls_title, ['fire chief']):
        ret_value = 'f15'
    elif fnmatch.fnmatch(ls_title, 'general'):
        ret_value = 'f16'
    elif find(ls_title, ['gov']):
        ret_value = 'f17'
    elif find(ls_title, ['investigator']):
        ret_value = 'f18'
    elif find(ls_title, ['lieutenant']):
        ret_value = 'f21'
    elif fnmatch.fnmatch(ls_title, 'major'):
        ret_value = 'f23'
    elif find(ls_title, ['marshall']):
        ret_value = 'f25'
    elif find(ls_title, ['paramedic', ' emt ', 'paramedic']) or re_find_list(ls_title,
                                                                             ['emt *', '* emt',
                                                                              '*emer* med* tech*']) or \
            fnmatch.fnmatch(ls_title, 'emt'):
        ret_value = 'f29'
    elif find(ls_title, ['police chief', 'chief of police']):
        ret_value = 'f30'
    elif find(ls_title, ['probation off']):
        ret_value = 'f32'
    elif find(ls_title, ['sergeant']):
        ret_value = 'f33'
    elif find(ls_title, ['sheriff']):
        ret_value = 'f34'
    elif re_find_list(ls_title, ['*trans* dep*', '*dep* of trans*']):
        ret_value = 'f37'
    elif re_find_list(ls_title, ['*treasury* dep*', '*dep* of the treasury*']):
        ret_value = 'f38'
    elif find(ls_title, ['warden']):
        ret_value = 'f39'
    elif find(ls_title, ['warrant officer']):
        ret_value = 'f40'
    elif find(ls_title, ['police']):
        ret_value = 'f41'
    elif find(ls_title, ['fireman', 'firefighter', 'fire fighter']) or re_find(ls_title,
                                                                               '*fire man') or fnmatch.fnmatch(
            ls_title, 'fire man'):
        ret_value = 'f42'
    elif find(ls_title, ['attorney', ' atty ', 'lawyer', 'member (of the bar)']):
        ret_value = 'g01'
    elif find(ls_title, ['judge']):
        ret_value = 'g03'
    elif find(ls_title, ['paralegal', 'para legal']):
        ret_value = 'g04'

    elif find(ls_title, ['legal counsel', 'general counsel', 'corporate counsel', 'of counsel',
                         'special counsel']) or fnmatch.fnmatch(ls_title, 'counsel'):
        ret_value = 'g05'

    elif find(ls_title, ['patent']):
        ret_value = 'g06'
    elif find(ls_title, ['trademark', 'trade mark']):
        ret_value = 'g07'
    elif find(ls_title, ['licensing']):
        ret_value = 'g08'
    elif find(ls_title, ['compliance']):
        ret_value = 'g09'
    elif find(ls_title, ['legal']):
        ret_value = 'g10'
    elif find(ls_title, ['salesman', 'sales man ', 'salesperson', 'sales woman']) or re_find(
            ls_title, '*sales man'):
        ret_value = 'h01'
    elif find(ls_title,
              ['account exe', 'acct exe', 'acct m', 'account m', 'account super',
               'acct super']) or fnmatch.fnmatch(ls_title, 'ae'):
        ret_value = 'h02'
    elif find(ls_title, ['sales rep']):
        ret_value = 'h03'
    elif find(ls_title, ['telemark']):
        ret_value = 'h04'
    elif find(ls_title, ['retail sale']):
        ret_value = 'h06'
    elif find(ls_title, ['sales']):
        ret_value = 'h05'
    elif find(ls_title, ['mktg', 'marketing', 'marketer']) or ls_title == 'cmo':
        ret_value = 'i01'
    elif find(ls_title,
              ['customer serv', 'customer rep', 'customer care', 'customer support', 'customer svc',
               'consumer rel',
               'customer rel', 'cust serv', 'cust rep', 'crm']):
        ret_value = 'j01'
    elif find(ls_title, ['insur']):
        ret_value = 'k01'
    elif find(ls_title, ['actuar']):
        ret_value = 'k02'
    elif find(ls_title, ['underwrit', 'under writ']):
        ret_value = 'k03'
    elif find(ls_title, ['risk m']):
        ret_value = 'k04'
    elif re_find(ls_title, '*cert* life* under*'):
        ret_value = 'k05'
    elif find(ls_title, ['buyer', 'buying']):
        ret_value = 'm01'
    elif find(ls_title, ['purchasing', 'purchaser', 'purch ', ' purch ', 'purchase exec',
                         'purch']) or re_find(ls_title, ' purch'):
        ret_value = 'm02'
    elif find(ls_title, ['procur']):
        ret_value = 'm03'
    elif re_find(ls_title, '*aerona* eng*'):
        ret_value = 'n01'
    elif re_find(ls_title, '*chem* eng*'):
        ret_value = 'n02'
    elif re_find(ls_title, '*struc* eng*'):
        ret_value = 'n04'
    elif find(ls_title, ['archtectural eng']):
        ret_value = 'n05'
    elif find(ls_title, ['cvl eng', 'civil eng']):
        ret_value = 'n06'
    elif re_find(ls_title, '*mech* eng*'):
        ret_value = 'n07'
    elif find(ls_title, ['mine eng', 'mining eng']):
        ret_value = 'n08'
    elif find(ls_title, ['eng']) and (
            find(ls_title, ['dev', 'design', 'dsgn', ' des ', 'dvlp']) or (
            re_find(ls_title, 'des *'))):
        ret_value = 'n09'
    # 07/07 - Ram - Rewrite the condition to match SQL Query
    #     WHEN @M_TEXT_TITLE LIKE '%MANU% eng%'
    #     OR @M_TEXT_TITLE LIKE 'MAN eng%'
    #     OR (@M_TEXT_TITLE LIKE '%MFG%' AND @M_TEXT_TITLE LIKE '% eng%')
    #     OR @M_TEXT_TITLE LIKE '% MAN eng%' THEN 'N10'
    elif re_find_list(ls_title, ['*manu* eng*', 'man eng*']) or (
            find(ls_title, ['mfg']) and find(ls_title, [' eng'])) or find(ls_title, [' man eng']):
        ret_value = 'n10'
    elif find(ls_title, ['power eng']):
        ret_value = 'n11'
    elif find(ls_title, ['production eng', 'prdtn eng']):
        ret_value = 'n12'
    elif re_find(ls_title, '*ind* eng*') and (not find(ls_title, ['window'])):
        ret_value = 'n13'
    elif re_find(ls_title, '*packag* eng*'):
        ret_value = 'n14'
    elif find(ls_title, ['comp eng', 'computer eng']):
        ret_value = 'n15'
    elif re_find_list(ls_title, ['*app* eng*', 'ap eng*']) or find(ls_title, [' ap eng']):
        ret_value = 'n17'
    elif re_find(ls_title, '*elec* eng*'):
        ret_value = 'n03'
    elif chk_in(ls_title, ['engineer', 'eng', 'professional engineer', 'engineering manager',
                           'senior engineer', 'principal engineer', 'mechanical engineer',
                           'process engineer', 'manufacturing engineer', 'staff engineer',
                           'sr engineer', 'industrial engineer']):
        ret_value = 'n16'

    elif (re_find_list(ls_title, ['eng *', '* eng']) or chk_in(ls_title, ['eng']) or find(ls_title,
                                                                                          [' eng ',
                                                                                           'engineer',
                                                                                           'engr'])) and (
            ((not find(ls_title, ['soft'])) and (not find(ls_title, ['sales'])) and (
                    not find(ls_title, ['applica'])) and (not find(ls_title, ['net'])) and (
                     not find(ls_title, ['sys'])) and (not find(ls_title, ['it'])) and (
                     not find(ls_title, ['sw '])) or (not find(ls_title, ['app'])))):
        ret_value = 'n16'
    elif re_find(ls_title, '*corp* ed*') and not find(ls_title, ['edi']):
        ret_value = 'o01'
    elif re_find_list(ls_title, ['*corp* trg*', '*corp* train*']):
        ret_value = 'o02'
    elif find(ls_title, ['exe dev', 'exec dev', 'executive dev']):
        ret_value = 'o03'
    elif find(ls_title, ['mgmt dev', 'manager dev', 'management dev']):
        ret_value = 'o04'
    elif find(ls_title, ['staff dev']):
        ret_value = 'o05'
    elif find(ls_title, ['training', 'trainer', 'trg', 'trng', 'trnr']) and (
            not find(ls_title, ['athletic'])):
        ret_value = 'o06'
    elif find(ls_title, ['real estate broker', 'real estate ag', 'realtor', 'realtors']):
        ret_value = 'p01'
    elif re_find(ls_title, '*prop* m*') and not find(ls_title, ['propo']):
        ret_value = 'p02'
    elif find(ls_title, ['land dev']):
        ret_value = 'p03'
    elif find(ls_title, ['real estate']):
        ret_value = 'p04'
    elif find(ls_title, ['property']):
        ret_value = 'p05'
    elif find(ls_title, ['meeting pl', 'mtg pl']):
        ret_value = 'q06'
    elif re_find(ls_title, '*material* pl*'):
        ret_value = 'q05'
    elif find(ls_title, ['corp pl', 'corporate pl']):
        ret_value = 'q04'
    elif find(ls_title, ['site pl', 'facility pl']):
        ret_value = 'q03'
    elif find(ls_title, ['financial pl', 'fin pl']):
        ret_value = 'q02'
    elif find(ls_title, ['plann']):
        ret_value = 'q01'
    elif find(ls_title, ['superin']) and find(ls_title, ['scho']):
        ret_value = 'r22'
    elif re_find_list(ls_title, ['*contin* ed*', '*cont* edu*']):
        ret_value = 'r05'
    elif find(ls_title, ['professor', 'prof of ']):
        ret_value = 'r06'
    elif find(ls_title, ['instructor']):
        ret_value = 'r07'
    elif find(ls_title, ['guidance counsel']):
        ret_value = 'r02'
    elif re_find(ls_title, '*sub* teacher*'):
        ret_value = 'r11'
    elif re_find(ls_title, '*teacher* aide*'):
        ret_value = 'r10'
    elif find(ls_title, ['dean of students', 'dean']) or (ls_title == 'dean'):
        ret_value = 'r12'
    elif find(ls_title, ['curriculum super']):
        ret_value = 'r13'
    elif (find(ls_title, ['edu']) and find(ls_title, ['consult'])) or (
            re_find(ls_title, 'ed *') and find(ls_title, ['consult'])) or (
            find(ls_title, [' ed ']) and find(ls_title, ['consult'])):
        ret_value = 'r14'
    elif (ls_title == 'student') or re_find_list(ls_title, ['* student', 'student of *']) or find(
            ls_title, ['students']):
        ret_value = 'r16'
    elif find(ls_title, ['registrar']):
        ret_value = 'r19'
    elif find(ls_title, ['bursar']):
        ret_value = 'r20'
    elif find(ls_title, ['librar', 'reference/information libr', 'bishop']):
        ret_value = 'r03'
    elif find(ls_title,
              ['educ', 'lecture', 'st grade', 'nd grade', 'rd grade', 'th grade', 'academic',
               'edu ', 'school', 'college', 'university']):
        ret_value = 'r21'
    elif find(ls_title, ['import']) and find(ls_title, ['export']):
        ret_value = 's03'
    elif find(ls_title, ['import']):
        ret_value = 's02'
    elif find(ls_title, ['import']):
        ret_value = 's02'
    elif find(ls_title, ['export']):
        ret_value = 's01'
    elif find(ls_title, ['brnch', 'branch']) and find(ls_title, ['intl', 'internat']):
        ret_value = 's04'
    elif find(ls_title, ['headq', 'head q']) and find(ls_title, ['intl', 'internat']):
        ret_value = 's05'
    # 07/07 - Ram - Rewrite the condition to match SQL Query
    # WHEN @M_TEXT_TITLE LIKE '%FOOD%'
    #  AND @M_TEXT_TITLE LIKE '%SERV%'
    #   or @M_TEXT_TITLE  LIKE '%Chef%'
    #   THEN 'T02'
    elif find(ls_title, ['food']) and find(ls_title, ['serv']) or find(ls_title, ['chef']):
        ret_value = 't02'
    elif find(ls_title, ['hospitality']) and find(ls_title, ['serv']):
        ret_value = 't03'
    elif find(ls_title, ['pub']) and find(ls_title, ['serv']) and not find(ls_title, ['publis']):
        ret_value = 't04'

    elif find(ls_title, ['serv ', 'service']) or re_find(ls_title, '* serv'):
        ret_value = 't01'
    elif find(ls_title, ['media']) and not find(ls_title, ['multi']):
        ret_value = 'u10'
    elif find(ls_title, ['video']):
        ret_value = 'u09'
    elif find(ls_title, ['broadcast', 'broad cast']):
        ret_value = 'u07'
    # 07/07 - Ram - Rewrite the condition to match SQL Query
    # WHEN @M_TEXT_TITLE LIKE '%audio%'
    #  OR @M_TEXT_TITLE LIKE '% A V %'
    #  OR @M_TEXT_TITLE LIKE '% AV %'
    #  OR @M_TEXT_TITLE LIKE 'A V %'
    #  OR @M_TEXT_TITLE LIKE '% A V'
    #  OR @M_TEXT_TITLE LIKE 'AV %'
    #  OR @M_TEXT_TITLE LIKE 'AV'
    #  OR @M_TEXT_TITLE LIKE 'A V'
    #  THEN 'U06'
    elif find(ls_title, ['audio', ' a v ', ' av ']) or re_find_list(ls_title, ['* a v',
                                                                               'a v *',
                                                                               'av *']) or chk_in(
            ls_title, ['av', 'a v']):
        ret_value = 'u06'
    elif find(ls_title, ['motion picture', 'movie']):
        ret_value = 'u05'
    elif (find(ls_title, ['tele com', 'telecom'])) and not find(ls_title, ['commuter']):
        ret_value = 'u04'
    elif find(ls_title, ['television', ' tv ']) or re_find_list(ls_title, ['* tv',
                                                                           'tv *']) or (
            ls_title == 'tv'):
        ret_value = 'u03'
    elif find(ls_title, ['radio']):
        ret_value = 'u02'
    elif re_find_list(ls_title, ['* comm', 'comm *']) or find(ls_title, [' comm ', 'communcation',
                                                                         'communications']) or (
            ls_title == 'comm'):
        ret_value = 'u01'
    elif find(ls_title, ['database', ' db ', 'sql', 'oracle', 'sybase', ' dba ']) or re_find_list(
            ls_title, ['db *', 'dba *', '* dba']) or ls_title == 'dba':
        ret_value = 'v28'
    elif find(ls_title, ['network']):
        ret_value = 'v27'
    elif re_find(ls_title, '*info* sec*'):
        ret_value = 'v30'
    elif find(ls_title, ['programmer', 'software', 'prgmr', 'lead developer', 'programming',
                         'java developer']) or re_find(ls_title, 'sw *'):
        ret_value = 'v29'
    elif find(ls_title, ['dp systems']):
        ret_value = 'v04'
    elif find(ls_title, ['dp program']):
        ret_value = 'v04'
    elif find(ls_title, ['dp software']):
        ret_value = 'v04'
    elif re_find(ls_title, '*dp tech* support*'):
        ret_value = 'v04'
    elif find(ls_title, ['dp hardware']):
        ret_value = 'v04'
    elif re_find(ls_title, '*dp communication* sys*'):
        ret_value = 'v04'
    elif re_find(ls_title, '*dp communication* equip*'):
        ret_value = 'v04'
    elif find(ls_title, ['dp communication']):
        ret_value = 'v04'
    elif re_find(ls_title, '*dp info* sys*'):
        ret_value = 'v04'
    elif find(ls_title, ['dp communication']):
        ret_value = 'v04'
    elif find(ls_title, ['word proc']):
        ret_value = 'v25'
    elif find(ls_title, ['data proc']):
        ret_value = 'v04'
    elif find(ls_title, ['desktop publ']):
        ret_value = 'v05'
    elif find(ls_title, ['data entry']):
        ret_value = 'v03'
    elif find(ls_title, ['computer op', 'comp op']):
        ret_value = 'v02'
    elif find(ls_title, ['internet']):
        ret_value = 'v08'
    elif re_find(ls_title, '*sys* integ*'):
        ret_value = 'v20'
    elif re_find(ls_title, '*sys* anal*'):
        ret_value = 'v19'
    elif re_find(ls_title, '*sys* prog*'):
        ret_value = 'v21'
    elif re_find(ls_title, '*tech* consult*'):
        ret_value = 'v22'
    elif find(ls_title, ['tech', 'technical']) and not find(ls_title, ['med']) and not find(
            ls_title, ['lab']):
        ret_value = 'v23'
    elif find(ls_title, ['tech']) and (find(ls_title, ['bio', 'lab'])) and not find(ls_title, [
        'comp']) or find(ls_title, ['metrologist', 'meteorologist', 'toxicologist', 'gemologist',
                                    'entomologist', 'ecologist']):
        ret_value = 'w01'
    elif find(ls_title, ['analy']):
        ret_value = 'v01'
    elif (re_find_list(ls_title,
                       ['it *', 'mis *', '*info* tech*', '*system* analyst*', '*info* sec*', 'it/*',
                        'data *', 'systems *', '*info* off*', 'is *', 'pc *']) or find(ls_title,
                                                                                       [' mis ',
                                                                                        'database',
                                                                                        'computer',
                                                                                        'applications',
                                                                                        'computing',
                                                                                        'technology',
                                                                                        'info sys',
                                                                                        ' is ',
                                                                                        'sys admin',
                                                                                        'sysadmin',
                                                                                        'unix',
                                                                                        'linux',
                                                                                        'websphere',
                                                                                        ' pc ',
                                                                                        ' it ']) and (
                  not re_find(ls_title, '* pc'))) and find(ls_title, ['consult']):
        ret_value = 'v10'

    elif ((re_find_list(ls_title,
                        ['it *', 'mis *', '*info* tech*', '*system* analyst*', '*info* sec*',
                         'it/*', 'data *', 'systems *', '*info* off*', 'is *', 'pc *']) or find(
        ls_title,
        [' mis ', 'database', 'computer', 'applications', 'computing', 'technology', 'info sys',
         ' is ', 'sys admin', 'sysadmin', 'unix', 'linux', 'websphere', ' pc ',
         ' it ']) or ls_title == 'it' or ls_title == 'mis' or chk_in(ls_title,
                                                                     ['cso', 'ciso']))) and (
            not re_find(ls_title, '* pc')) and (find(ls_title, ['devel', 'eng'])):
        ret_value = 'v11'
    elif ((re_find_list(ls_title,
                        ['it *', 'mis *', '*info* tech*', '*system* analyst*', '*info* sec*',
                         'it/*', 'data *', 'systems *', '*info* off*', 'is *',
                         'pc *']) or ls_title == 'it' or ls_title == 'mis' or chk_in(ls_title,
                                                                                     ['cso',
                                                                                      'ciso']) or find(
        ls_title,
        [' mis ', 'database', 'computer', 'applications', 'computing', 'technology', 'info sys',
         ' is ', 'sys admin', 'sysadmin', 'unix', 'linux', 'websphere', ' pc ', ' it '])) and (
                  not re_find(ls_title, '* pc'))) and find(ls_title, ['operator']):
        ret_value = 'v13'
    # WHEN @M_TEXT_TITLE = 'mis'
    #   OR @M_TEXT_TITLE LIKE '% mis %'
    #   OR @M_TEXT_TITLE LIKE '%info sys%'
    #   OR @M_TEXT_TITLE LIKE '% IS %'

    #   OR @M_TEXT_TITLE LIKE '% mis'
    #   OR @M_TEXT_TITLE LIKE '% is'
    #   OR @M_TEXT_TITLE_Orginal LIKE '%-mis'
    #   OR @M_TEXT_TITLE LIKE 'mis %'
    #   OR @M_TEXT_TITLE LIKE 'IS %'
    #   or @M_TEXT_TITLE LIKE '%info% off%'

    #   or @M_TEXT_TITLE  = 'Is'

    #   THEN 'V17'
    elif chk_in(ls_title, ['mis', 'is']) or find(ls_title,
                                                 [' mis ', 'info sys', ' is ']) or re_find_list(
            ls_title, ['mis *', '* mis', 'is *', '* is', '*info* off*']) or re_find(ls_title_orignal, '*-mis'):
        ret_value = 'v17'
    # WHEN ((
    # 		@M_TEXT_TITLE LIKE 'it %'
    # 		OR @M_TEXT_TITLE LIKE '% IT'
    # 		OR @M_TEXT_TITLE_Orginal LIKE '%-IT'
    # 		OR @M_TEXT_TITLE LIKE '% CTO'
    # 		OR @M_TEXT_TITLE LIKE '% CIO'
    # 		OR @M_TEXT_TITLE_Orginal LIKE 'CTO, %'
    # 		OR @M_TEXT_TITLE_Orginal LIKE 'CTO %'
    # 		OR @M_TEXT_TITLE_Orginal LIKE 'CIO, %'
    # 		OR @M_TEXT_TITLE_Orginal LIKE 'CIO %' --
    # 		OR @M_TEXT_TITLE IN('IT','CIO','CTO')
    # 		OR @M_TEXT_TITLE = 'mis'
    # 		OR @M_TEXT_TITLE LIKE '% mis %'
    # 		OR @M_TEXT_TITLE LIKE 'mis %'--
    # 		OR @M_TEXT_TITLE LIKE '%info% tech%'--
    # 		OR @M_TEXT_TITLE LIKE '%database%'
    # 		OR @M_TEXT_TITLE LIKE '%system% analyst%'--
    # 		OR @M_TEXT_TITLE in ('cso','CISO')
    # 		OR @M_TEXT_TITLE LIKE '%info% sec%'--
    # 		OR @M_TEXT_TITLE LIKE 'it/%'--
    # 		OR @M_TEXT_TITLE LIKE '%computer%'
    # 		OR @M_TEXT_TITLE LIKE 'data %'--
    # 		OR @M_TEXT_TITLE LIKE '%APPLICATIONS%'
    # 		OR @M_TEXT_TITLE LIKE 'SYSTEMS %'--
    # 		OR @M_TEXT_TITLE LIKE '%COMPUTING%'
    # 		OR @M_TEXT_TITLE LIKE '%TECHNOLOGY%'
    # 		OR @M_TEXT_TITLE LIKE '%info sys%'
    # 		OR @M_TEXT_TITLE LIKE '%info% off%'
    # 		OR @M_TEXT_TITLE LIKE 'IS %'
    # 		OR @M_TEXT_TITLE LIKE '% IS %'
    # 		OR @M_TEXT_TITLE LIKE '%SYS ADMIN%'
    # 		OR @M_TEXT_TITLE LIKE '%SYSADMIN%'
    # 		OR @M_TEXT_TITLE LIKE '%UNIX%'
    # 		OR @M_TEXT_TITLE LIKE '%LINUX%'
    # 		OR @M_TEXT_TITLE LIKE '%WEBSPHERE%'
    # 		OR @M_TEXT_TITLE LIKE 'PC %'
    # 		OR @M_TEXT_TITLE LIKE '% PC %'
    # 		OR @M_TEXT_TITLE LIKE '% it %')
    # 		AND (@M_TEXT_TITLE NOT LIKE '% PC'
    # 	  ))
    # 		OR @M_TEXT_TITLE  LIKE '%Head of IT%' THEN 'V09'

    elif (((re_find_list(ls_title,
                        ['it *', '* it', '* cto', '* cio',
                         '*info* tech*', 'mis *', '*system* analyst*', '*info* sec*',
                         'it/*', 'data *', 'systems *', '*info sys*', '*info* off*', 'is *',
                         'pc *']) or re_find_list(ls_title_orignal,
                                                  ['*-it', 'cto, *', 'cto *', 'cio, *', 'cio *']) or find(ls_title,
                                                                                                  [' mis ',
                                                                                                   'database',
                                                                                                   'computer',
                                                                                                   'applications',
                                                                                                   'computing',
                                                                                                   'technology',
                                                                                                   'info sys',
                                                                                                   ' is ',
                                                                                                   'sys admin',
                                                                                                   'sysadmin',
                                                                                                   'unix',
                                                                                                   'linux',
                                                                                                   'websphere',
                                                                                                   ' pc ',
                                                                                                   ' it ']) or chk_in(
            ls_title, ['it', 'cio', 'cto', 'cso', 'ciso']) or (ls_title == 'mis')) and (
                   not re_find(ls_title, '* pc')))) or find(ls_title, ['head of it']):
        ret_value = 'v09'
    elif find(ls_title, ['app ', 'application']):
        ret_value = 'v31'
    elif find(ls_title, ['web mast', 'webmast', 'web adm', 'web dev']):
        ret_value = 'v24'
    elif find(ls_title, ['system', ' sys ']) or re_find_list(ls_title, ['sys *', '* sys']):
        ret_value = 'v26'
    elif find(ls_title, ['chemist']):
        ret_value = 'w12'
    elif find(ls_title, ['physics', 'physicist']):
        ret_value = 'w11'
    elif find(ls_title, ['physical sci']):
        ret_value = 'w10'
    elif find(ls_title, ['metallurg']):
        ret_value = 'w09'
    elif find(ls_title, ['petrochem', 'petro chem']) and not find(ls_title, ['eng']):
        ret_value = 'w07'
    elif find(ls_title, ['chemical']) and (not find(ls_title, ['eng'])):
        ret_value = 'w08'
    elif find(ls_title, ['energy']):
        ret_value = 'w06'
    elif find(ls_title, ['environ', ' env ']) or re_find(ls_title, 'env *'):
        ret_value = 'w14'
    elif find(ls_title, ['oceanogr']):
        ret_value = 'w05'
    elif find(ls_title, ['geolog']):
        ret_value = 'w04'
    elif re_find(ls_title, 'ag *') or find(ls_title, ['agri']):
        ret_value = 'w03'
    elif find(ls_title, ['aviator', 'aviation', 'aero']):
        ret_value = 'w02'
    elif find(ls_title, ['scienc', ' sci ', 'scientist', 'scientif']) or re_find(ls_title, 'sci *'):
        ret_value = 'w01'
    elif fnmatch.fnmatch(ls_title, 'adv') or find(ls_title, ['advert']):
        ret_value = 'l01'
    elif re_find_list(ls_title, ['*pub* rel*', '* of pr', 'pr *']) or ls_title == 'pr':
        ret_value = 'x01'

    elif find(ls_title, ['publicity', 'press rel']):
        ret_value = 'x02'
    elif find(ls_title, ['community rel']):
        ret_value = 'x03'

    elif find(ls_title, ['fabricat', 'manu ', 'manuf', 'mfg']):
        ret_value = 'y01'

    elif fnmatch.fnmatch(ls_title, 'qa') or re_find_list(ls_title,
                                                         ['qa *', '* qa', 'qual assu*']) or find(
            ls_title, [' qa ', 'qlty assu']):
        ret_value = 'z03'

    elif find(ls_title, ['qc', 'qual cont', 'qlty cont']):
        ret_value = 'z01'

    elif find(ls_title, ['quality', 'qlty']):
        ret_value = 'z04'

    elif re_find(ls_title, '*equip* main*'):
        ret_value = '102'
    elif find(ls_title, ['building main']):
        ret_value = '101'
    elif find(ls_title, ['janitor', 'custodi']):
        ret_value = '104'
    elif find(ls_title, ['maint']):
        ret_value = '103'
    elif chk_in(ls_title, ['architect', 'professional architect', 'chief architect', 'arch']):
        ret_value = '201'
    elif find(ls_title, ['draft']):
        ret_value = '202'
    elif re_find_list(ls_title, ['*int* design*', '*int* decorat*', '*int* des*']):
        ret_value = '203'
    elif find(ls_title, ['wholesale']):
        ret_value = '311'
    elif find(ls_title, ['warehous']):
        ret_value = '310'
    elif find(ls_title, ['shipping']):
        ret_value = '306'
    elif find(ls_title, ['receiving']):
        ret_value = '305'
    elif find(ls_title, ['inventory']):
        ret_value = '302'
    elif re_find(ls_title, '*mat* handl*'):
        ret_value = '303'
    elif find(ls_title, ['plant']) and not find(ls_title, ['planter']):
        ret_value = '401'
    elif find(ls_title, ['facility', ' fac ', 'facilities']) or re_find(ls_title,
                                                                        'fac *'):
        ret_value = '402'
    elif find(ls_title, ['building']):
        ret_value = '403'
    elif find(ls_title, ['foreman']):
        ret_value = '404'
    elif find(ls_title, [' asst', ' assistant']) and (find(ls_title, ['adm'])):
        ret_value = '501'
    elif find(ls_title, [' asst', ' assistant']) and find(ls_title, ['exec']):
        ret_value = '502'
    elif find(ls_title, ['secretary to ', 'sec to ']):
        ret_value = '504'
    elif find(ls_title, ['reception']):
        ret_value = '505'
    elif find(ls_title, ['office ']):
        ret_value = '506'
    elif find(ls_title, ['traffic']):
        ret_value = '308'
    elif find(ls_title, ['air trans']):
        ret_value = '301'
    elif find(ls_title, ['trans ', 'transpo']):
        ret_value = '309'
    elif find(ls_title, ['advis']):
        ret_value = '601'
    elif find(ls_title, ['agent']):
        ret_value = '602'
    elif find(ls_title, ['assemb']):
        ret_value = '605'
    elif ls_title == 'assistant':
        ret_value = '606'
    elif ls_title == 'developer':
        ret_value = '655'
    elif chk_in(ls_title, ['admin', 'administrator']):
        ret_value = '658'
    elif chk_in(ls_title, ['principal', 'principle']):
        ret_value = '660'
    elif find(ls_title, ['attendant']):
        ret_value = '607'
    elif find(ls_title, ['broker']):
        ret_value = '608'
    elif find(ls_title, ['cashier']):
        ret_value = '609'

    elif find(ls_title,
              ['clergy', 'pastor ', 'reverend', 'imam', 'rabbi', 'minister', 'bishop',
               'youth pastor',
               'pastors', 'chaplain', 'deacons', 'preacher', 'ministers', 'childrens minister',
               'minister of youth', 'student ministries']) or fnmatch.fnmatch(ls_title, 'pastor'):
        ret_value = '610'

    elif find(ls_title, ['clerk']):
        ret_value = '611'
    elif fnmatch.fnmatch(ls_title, 'consultant'):
        ret_value = '613'
    elif find(ls_title, ['consultant']):
        ret_value = '614'
    elif find(ls_title, ['correspondent']):
        ret_value = '615'
    elif find(ls_title, ['dispatch']):
        ret_value = '617'
    elif find(ls_title, ['distance learn']):
        ret_value = '618'
    elif find(ls_title, ['examiner']):
        ret_value = '619'
    elif find(ls_title, ['faciliator']):
        ret_value = '620'
    elif re_find(ls_title, 'gen* m*') or find(ls_title, ['general m']):
        ret_value = '622'
    elif find(ls_title, ['inspector']):
        ret_value = '623'
    elif find(ls_title, ['install']):
        ret_value = '624'
    elif find(ls_title, ['leader']):
        ret_value = '625'
    elif find(ls_title, ['liason']):
        ret_value = '626'
    elif find(ls_title, ['line manage', 'line mg']):
        ret_value = '628'
    elif find(ls_title, ['mailroom', 'mail room']):
        ret_value = '629'
    elif find(ls_title, ['mailing']) and not find(ls_title, ['email']):
        ret_value = '627'
    elif find(ls_title, ['merchandis']):
        ret_value = '630'

    elif find(ls_title, ['mining', 'mineral']) and (not find(ls_title, ['data'])):
        ret_value = '631'

    elif find(ls_title, ['operation', ' ops ', 'opers']) or re_find_list(ls_title, ['* ops',
                                                                                    'ops *']) or fnmatch.fnmatch(
            ls_title, 'ops'):
        ret_value = '632'

    elif find(ls_title, ['operator']):
        ret_value = '633'
    elif find(ls_title, ['organizer']):
        ret_value = '634'
    elif find(ls_title, ['packaging']):
        ret_value = '635'
    elif find(ls_title, ['processor']):
        ret_value = '635'
    elif find(ls_title, ['product dev']):
        ret_value = '638'

    elif find(ls_title, ['production', 'prdtn']):
        ret_value = '639'

    elif find(ls_title, ['recreation']):
        ret_value = '641'
    # 07/07 - Ram - Rewrite the condition to match SQL Query
    # WHEN @M_TEXT_TITLE LIKE '%RESEARCH % DEV%'
    #   OR @M_TEXT_TITLE LIKE '%RES % DEV%'
    #   OR @M_TEXT_TITLE LIKE 'R&D %'
    #   OR @M_TEXT_TITLE LIKE '% R&D'
    #   OR @M_TEXT_TITLE LIKE 'R & D %'
    #   OR @M_TEXT_TITLE LIKE '% R & D'
    #   OR @M_TEXT_TITLE LIKE 'R AND D %'
    #   OR @M_TEXT_TITLE LIKE '% R AND D'

    #   OR @M_TEXT_TITLE LIKE 'R AND D'
    #   OR @M_TEXT_TITLE LIKE 'R&D'
    #   OR @M_TEXT_TITLE LIKE 'R & D'

    #   OR @M_TEXT_TITLE LIKE '% R&D %'
    #   OR @M_TEXT_TITLE LIKE '% R & D %'
    #   OR @M_TEXT_TITLE LIKE '% R AND D %'

    #   THEN '642'
    elif re_find_list(ls_title,
                      ['*research * dev*', '*res * dev*', 'r&d *', '* r&d', 'r & d *', '* r & d',
                       'r and d *', '* r and d']) or find(ls_title,
                                                          [' r&d ', ' r & d ',
                                                           ' r and d ']) or chk_in(
            ls_title, ['r and d', 'r&d', 'r & d']):
        ret_value = '642'

    elif find(ls_title, ['safety']):
        ret_value = '643'
    elif find(ls_title, ['sanitation']):
        ret_value = '645'
    elif chk_in(ls_title, ['security', 'security guard']):
        ret_value = '647'
    elif find(ls_title, ['specialist']):
        ret_value = '648'
    elif find(ls_title, ['staff']):
        ret_value = '649'
    elif find(ls_title, ['tradesman', 'craftsman']):
        ret_value = '652'
    elif find(ls_title, ['trainee']):
        ret_value = '654'
    elif find(ls_title, ['meeting', 'mtg']) or re_find(ls_title, '*conf* plan* '):
        ret_value = '656'
    elif find(ls_title, ['travel']):
        ret_value = '657'
    elif find(ls_title, ['coach']):
        ret_value = '659'
    elif find(ls_title, ['design', ' des ', 'dsgn']) or re_find(ls_title, 'des *'):
        ret_value = '661'
    elif find(ls_title, ['architect', ' arch ']) or re_find_list(ls_title, ['arch *', '* arch']):
        ret_value = '662'
    elif find(ls_title, ['farmer']):
        ret_value = '663'
    elif chk_in(ls_title, ['secretary', 'secy']):
        ret_value = '664'
    elif ls_title == 'mechanic':
        ret_value = '665'
    elif find(ls_title, ['photograph']):
        ret_value = '667'
    elif find(ls_title, ['producer']):
        ret_value = '668'
    elif find(ls_title, ['machinist']):
        ret_value = '669'

    elif ls_title == 'control':
        ret_value = '670'
    elif ls_title == 'materials':
        ret_value = '671'
    elif chk_in(ls_title, ['bookkeeper', 'book keeper', 'bookeeper']):
        ret_value = 'a36'
    elif ls_title == 'accounts':
        ret_value = 'a37'
    elif ls_title == 'billing':
        ret_value = 'a38'
    elif chk_in(ls_title, ['electrician', 'journeyman electrician']):
        ret_value = 'd09'
    elif ls_title == 'plumber':
        ret_value = 'd10'
    elif chk_in(ls_title, ['do - acupuncturist', 'acupuncturist']):
        ret_value = 'e40'
    elif chk_in(ls_title,
                ['cardiologist', 'cardiovascular disease', 'cardiology', 'cardiologists']):
        ret_value = 'e41'
    elif chk_in(ls_title, ['do - chiropractor', 'chiropractor', 'do - chiropractors',
                           'doctor of chiropractic']):
        ret_value = 'e42'
    elif ls_title == 'counselor':
        ret_value = 'e43'
    elif ls_title == 'do - degenerative diseases':
        ret_value = 'e44'
    elif chk_in(ls_title, ['do - naturopathic', 'dr - naturopathic', 'naturopathic']):
        ret_value = 'e45'
    elif ls_title == 'emergency medicine':
        ret_value = 'e46'
    elif chk_in(ls_title,
                ['endocrinology', 'endocrinology - diabetes metabolism', 'endocrinologist',
                 'endocrinologists',
                 'reproductive endocrinology and infertility', 'reproductive endocrinology',
                 'endocrinology - diabetes & metabolism', 'endocrinology & metabolism',
                 'endocrinologist/diabetes spec',
                 'reproductive endocrinology infertility']):
        ret_value = 'e47'
    elif chk_in(ls_title, ['ophthalmology', 'ophthalmologist', 'optometrist', 'do - optometry',
                           'optometrists',
                           'ophthalmologists', 'optometrist ophthalmologist', 'do - ophthalmology',
                           'doctor of optometry',
                           'do - ophthalmologist', 'ophthalmology m d', 'opthamologists']):
        ret_value = 'e48'
    elif chk_in(ls_title, ['rdh', 'dh', 'hygienist']):
        ret_value = 'e49'
    elif chk_in(ls_title, ['radiation oncology', 'oncology', 'oncologist', 'surgical oncologist',
                           'oncologists', 'radiation oncologist', 'radiation oncologists',
                           'surgical oncology']):
        ret_value = 'e50'
    elif ls_title == 'podiatrist':
        ret_value = 'e51'
    elif chk_in(ls_title, ['do - urology', 'urology', 'urologist']):
        ret_value = 'e52'
    elif chk_in(ls_title,
                ['business development', 'business development manag', 'business developer',
                 'business development mngr']):
        ret_value = 'h07'
    elif ls_title == 'account':
        ret_value = 'h08'
    elif chk_in(ls_title, ['promotions', 'promotion']):
        ret_value = 'i02'
    elif chk_in(ls_title, ['trustee', 'trustees']):
        ret_value = 'p06'
    elif chk_in(ls_title, ['faculty', 'adjunct faculty', 'faculty member', 'faculty biology',
                           'adjunct faculty psychology', 'faculty psychology']):
        ret_value = 'r23'
    elif chk_in(ls_title, ['microbiolog', 'microbiologist', 'microbiology researcher',
                           'research microbiologist', 'micro biologist']):
        ret_value = 'w15'
    elif chk_in(ls_title,
                ['biologist', 'research biologist', 'wildlife biologist', 'fisheries biologist',
                 'molecular biologist']):
        ret_value = 'w16'
    elif chk_in(ls_title, ['research', 'researcher', 'researchers']):
        ret_value = 'w17'
    return ret_value.upper()
