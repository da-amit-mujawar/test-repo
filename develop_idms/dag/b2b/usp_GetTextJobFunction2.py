import fnmatch


def usp_GetTextJobFunction2(ps_title):
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
    la_invalid_chars = [(',', ''), ('#', ''), ("'", ''), ('.', ''), ('-', ''), ('/', ' '),
                        ('\\', ' '), ('v p ', 'vp '), ('c e o ', 'ceo ')]

    for x in la_invalid_chars:
        ls_title = ls_title.replace(x[0], x[1])

    ls_title_no_space = str(ls_title.replace(' ', ''))

    la_null_values = ['programdirector', 'minimumdatasetcoordinator', 'federalprogramdirector',
                      'programdirector']

    if ls_title_no_space in la_null_values:
        ret_value = ''
    elif ls_title_no_space == 'directorofsocialservices':
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
    elif find(ls_title, ['machinist']):
        ret_value = '669'
    elif find(ls_title, ['producer']):
        ret_value = '668'
    elif find(ls_title, ['photograph']):
        ret_value = '667'
    elif ls_title == 'mechanic':
        ret_value = '665'
    elif chk_in(ls_title, ['secretary', 'secy']):
        ret_value = '664'
    elif find(ls_title, ['farmer']):
        ret_value = '663'
    elif find(ls_title, ['architect', ' arch ']) or re_find(ls_title, 'arch *') or re_find(
            ls_title, '* arch'):
        ret_value = '662'
    elif find(ls_title, ['design', ' des ', 'dsgn']) or re_find(ls_title, 'des *'):
        ret_value = '661'
    elif find(ls_title, ['coach']):
        ret_value = '659'
    elif find(ls_title, ['travel']):
        ret_value = '657'
    elif find(ls_title, ['meeting', 'mtg']) or re_find(ls_title, '*conf* plan* '):
        ret_value = '656'
    elif find(ls_title, ['trainee']):
        ret_value = '654'
    elif find(ls_title, ['tradesman', 'craftsman']):
        ret_value = '652'
    elif find(ls_title, ['staff']):
        ret_value = '649'
    elif find(ls_title, ['specialist']):
        ret_value = '648'
    elif chk_in(ls_title, ['security', 'security guard']):
        ret_value = '647'
    elif find(ls_title, ['sanitation']):
        ret_value = '645'
    elif find(ls_title, ['safety']):
        ret_value = '643'
    elif (ls_title in ['r and d', 'r&d', 'r & d']) or \
            find(ls_title, [' r&d ', ' r & d ', ' r and d ']) or \
            re_find_list(ls_title,
                         ['*research * dev*', '*res * dev*', 'r&d *', '* r&d', 'r & d *', '* r & d',
                          'r and d *', '* r and d']):
        ret_value = '642'
    elif find(ls_title, ['recreation']):
        ret_value = '641'
    elif find(ls_title, ['production', 'prdtn']):
        ret_value = '639'
    elif find(ls_title, ['product dev']):
        ret_value = '638'
    elif find(ls_title, ['processor']):
        ret_value = '635'
    elif find(ls_title, ['packaging']):
        ret_value = '635'
    elif find(ls_title, ['organizer']):
        ret_value = '634'
    elif find(ls_title, ['operator']):
        ret_value = '633'
    elif find(ls_title, ['operation', ' ops ', 'opers']) or \
            fnmatch.fnmatch(ls_title, 'ops') or \
            re_find_list(ls_title, ['* ops', 'ops *']):
        ret_value = '632'

    elif (find(ls_title, ['mining', 'mineral'])) and not find(ls_title, ['data']):
        ret_value = '631'
    elif find(ls_title, ['merchandis']):
        ret_value = '630'
    elif find(ls_title, ['mailing']) and not find(ls_title, ['email']):
        ret_value = '627'
    elif find(ls_title, ['mailroom', 'mail room']):
        ret_value = '629'
    elif find(ls_title, ['line manage', 'line mg']):
        ret_value = '628'
    elif find(ls_title, ['liason']):
        ret_value = '626'
    elif find(ls_title, ['leader']):
        ret_value = '625'
    elif find(ls_title, ['install']):
        ret_value = '624'
    elif find(ls_title, ['inspector']):
        ret_value = '623'
    elif re_find(ls_title, 'gen* m*') or find(ls_title, ['general m']):
        ret_value = '622'
    elif find(ls_title, ['faciliator']):
        ret_value = '620'
    elif find(ls_title, ['examiner']):
        ret_value = '619'
    elif find(ls_title, ['distance learn']):
        ret_value = '618'
    elif find(ls_title, ['dispatch']):
        ret_value = '617'
    elif find(ls_title, ['correspondent']):
        ret_value = '615'
    elif find(ls_title, ['consultant']):
        ret_value = '614'
    elif ls_title in ['consultant']:
        ret_value = '613'
    elif find(ls_title, ['clerk']):
        ret_value = '611'
    elif find(ls_title, ['clergy', 'pastor ', 'reverend', 'imam', 'rabbi']) or \
            fnmatch.fnmatch(ls_title, 'pastor'):
        ret_value = '610'
    elif find(ls_title, ['cashier']):
        ret_value = '609'
    elif find(ls_title, ['broker']):
        ret_value = '608'
    elif find(ls_title, ['attendant']):
        ret_value = '607'
    elif chk_in(ls_title, ['principal']):
        ret_value = '660'
    elif ls_title in ['admin', 'administrator']:
        ret_value = '658'
    elif ls_title == 'developer':
        ret_value = '655'
    elif ls_title == 'assistant':
        ret_value = '606'
    elif find(ls_title, ['assemb']):
        ret_value = '605'
    elif find(ls_title, ['agent']):
        ret_value = '602'
    elif find(ls_title, ['advis']):
        ret_value = '601'
    elif find(ls_title, ['trans ', 'transpo']):
        ret_value = '309'
    elif find(ls_title, ['air trans']):
        ret_value = '301'
    elif find(ls_title, ['traffic']):
        ret_value = '308'
    elif find(ls_title, ['office ']):
        ret_value = '506'
    elif find(ls_title, ['reception']):
        ret_value = '505'
    elif find(ls_title, ['secretary to ', 'sec to ']):
        ret_value = '504'
    elif (find(ls_title, [' asst', ' assistant'])) and (find(ls_title, ['exec'])):
        ret_value = '502'
    elif (find(ls_title, [' asst', ' assistant'])) and (find(ls_title, ['adm'])):
        ret_value = '501'
    elif find(ls_title, ['foreman']):
        ret_value = '404'
    elif find(ls_title, ['building']):
        ret_value = '403'
    elif find(ls_title, [' fac ', 'facility']) or re_find(ls_title, 'fac *'):
        ret_value = '402'
    elif find(ls_title, ['plant']) and not find(ls_title, ['planter']):
        ret_value = '401'
    elif re_find(ls_title, '*mat* handl*'):
        ret_value = '303'
    elif find(ls_title, ['inventory']):
        ret_value = '302'
    elif find(ls_title, ['receiving']):
        ret_value = '305'
    elif find(ls_title, ['shipping']):
        ret_value = '306'
    elif find(ls_title, ['warehous']):
        ret_value = '310'
    elif find(ls_title, ['wholesale']):
        ret_value = '311'
    elif re_find(ls_title, '*int* design*') or re_find(ls_title, '*int* decorat*') or re_find(
            ls_title, '*int* des*'):
        ret_value = '203'

    elif find(ls_title, ['draft']):
        ret_value = '202'
    elif chk_in(ls_title, ['architect', 'professional architect', 'chief architect', 'arch']):
        ret_value = '201'
    elif find(ls_title, ['maint']):
        ret_value = '103'
    elif find(ls_title, ['janitor', 'custodi']):
        ret_value = '104'
    elif find(ls_title, ['building main']):
        ret_value = '101'
    elif re_find(ls_title, '*equip* main*'):
        ret_value = '102'
    # change made to z04
    elif find(ls_title, ['quality', 'qlty']):
        ret_value = 'z04'
    # change made to z01
    elif find(ls_title, ['qc']) or re_find_list(ls_title, ['*qual cont*', '*qlty cont*']):
        ret_value = 'z01'

    # change made to z03
    elif fnmatch.fnmatch(ls_title, 'qa') or \
            re_find_list(ls_title, ['qa *', '* qa', 'qual assu*', '*qlty assu*']) or \
            find(ls_title, [' qa ']):
        ret_value = 'z03'
    # change made to y01
    elif find(ls_title, ['fabricat', 'manu ', 'manuf', 'mfg']):
        ret_value = 'y01'

    elif find(ls_title, ['community rel']):
        ret_value = 'x03'
    elif find(ls_title, ['publicity', 'press rel']):
        ret_value = 'x02'
    elif re_find_list(ls_title, ['*pub* rel*', '* of pr', 'pr *']) or ls_title == 'pr':
        ret_value = 'x01'
    elif fnmatch.fnmatch(ls_title, 'adv') or re_find(ls_title, '*advert*'):
        ret_value = 'l01'
    elif find(ls_title, ['scienc', ' sci ', 'scientist', 'scientif']) or re_find(ls_title,
                                                                                 'sci *'):
        ret_value = 'w01'
    elif find(ls_title, ['aviator',
                         'aviation',
                         'aero']):
        ret_value = 'w02'
    elif re_find(ls_title, 'ag *') or find(ls_title, ['agri']):
        ret_value = 'w03'
    elif find(ls_title, ['geolog']):
        ret_value = 'w04'
    elif find(ls_title, ['oceanogr']):
        ret_value = 'w05'
    elif find(ls_title, ['environ', ' env ']) or re_find(ls_title, 'env *'):
        ret_value = 'w14'
    elif find(ls_title, ['energy']):
        ret_value = 'w06'
    elif find(ls_title, ['chemical']) and not find(ls_title, ['eng']):
        ret_value = 'w08'
    elif (find(ls_title, ['petrochem', 'petro chem'])) and not find(
            ls_title,
            ['eng']):
        ret_value = 'w07'
    elif find(ls_title, ['metallurg']):
        ret_value = 'w09'
    elif find(ls_title, ['physical sci']):
        ret_value = 'w10'
    elif find(ls_title, ['physics']) or find(ls_title, ['physicist']):
        ret_value = 'w11'
    elif find(ls_title, ['chemist']):
        ret_value = 'w12'
    elif find(ls_title, ['system', ' sys ']) or re_find_list(ls_title, ['sys *', '* sys']):
        ret_value = 'v26'
    elif find(ls_title, ['web mast', 'webmast', 'web adm', 'web dev']):
        ret_value = 'v24'
    elif find(ls_title, ['app ', 'application']):
        ret_value = 'v31'

    elif ((ls_title in ['it', 'mis', 'cso', 'ciso']) or
          re_find_list(ls_title,
                       ['mis *', 'it *', '*info* tech*', '*system* analyst*', '*info* sec*', 'it/*',
                        'data *', '*info* off*', 'is *', 'systems *', 'pc *']) or
          find(ls_title,
               ['applications', 'computer', 'computing', 'database', 'technology', 'info sys',
                ' is ', 'sys admin', ' mis ', 'sysadmin', 'unix', 'linux', 'websphere', ' pc ',
                ' it '])) and (
            not re_find(ls_title, '* pc')):
        ret_value = 'v09'
    elif (ls_title == 'mis' or
          find(ls_title, [' mis ', ' is ', 'info sys']) or
          re_find_list(ls_title, ['mis *', '*info* off*', 'is *'])):
        ret_value = 'v17'
    elif (re_find(ls_title, 'it *') or
          ls_title in ['it', 'mis', 'cso', 'ciso'] or
          re_find_list(ls_title,
                       ['mis *', '*info* tech*', '*system* analyst*', '*info* sec*', 'it/*',
                        'data *', 'systems *', '*info* off*', 'is *', 'pc *']) or
          find(ls_title,
               [' mis ', 'database', 'computer', 'applications', 'computing', 'technology',
                'info sys', ' is ', 'sys admin', 'sysadmin', 'unix', 'linux', 'websphere', ' pc ',
                ' it '])) and (not fnmatch.fnmatch(ls_title, '* pc')) and find(ls_title,
                                                                               ['operator']):
        ret_value = 'v13'
    elif (
            chk_in(ls_title, ['cso', 'ciso', 'it', 'mis']) or
            find(ls_title,
                 [' mis ', 'database', 'computer', 'applications', ' is ', 'unix', 'linux',
                  'sys admin', 'sysadmin', 'websphere', ' pc ', 'computing', 'technology',
                  'info sys', ' it ']) or
            re_find_list(ls_title,
                         ['mis *', 'it *', '*info* tech*', '*system* analyst*', '*info* sec*',
                          'it/*',
                          'data *', 'systems *', '*info* off*', 'is *', 'pc *'])) and (
            not fnmatch.fnmatch(ls_title, '* pc')) and (find(ls_title, ['devel', 'eng'])):
        ret_value = 'v11'
    elif (chk_in(ls_title, ['cso', 'it', 'mis', 'ciso']) or re_find_list(ls_title, ['it *', 'mis *',
                                                                                    '*info* tech*',
                                                                                    '*system* analyst*',
                                                                                    '*info* sec*',
                                                                                    'pc *',
                                                                                    'data *',
                                                                                    'is *',
                                                                                    'systems *',
                                                                                    '*info* off*',
                                                                                    'it/*']) or find(
        ls_title,
        [' mis ', 'database', 'computer', 'applications', ' is ', 'unix', 'linux', 'sysadmin',
         'sys admin', 'websphere', ' pc ', 'computing', 'technology', 'info sys',
         ' it '])) and (not fnmatch.fnmatch(ls_title, '* pc')) and fnmatch.fnmatch(ls_title,
                                                                                   '*consult*'):
        ret_value = 'v10'
    #  DATA ANALYST  PYTHON  BS IN APPLIED MATHEMATIC
    #     WHEN @M_TEXT_TITLE LIKE '%ANALY%' THEN 'V01'
    elif find(ls_title, ['analy']):
        ret_value = 'v01'
    elif find(ls_title, ['tech']) and (
            find(ls_title, ['bio']) or fnmatch.fnmatch(ls_title, '*lab*')) and not fnmatch.fnmatch(
        ls_title, '*comp*'):
        ret_value = 'w01'
    elif (find(ls_title, ['tech']) or fnmatch.fnmatch(ls_title,
                                                      '*technical*')) and not fnmatch.fnmatch(
        ls_title,
        '*med*') and not find(ls_title, ['lab']):
        ret_value = 'v23'
    elif fnmatch.fnmatch(ls_title, '*tech* consult*'):
        ret_value = 'v22'
    elif fnmatch.fnmatch(ls_title, '*sys* prog*'):
        ret_value = 'v21'
    elif fnmatch.fnmatch(ls_title, '*sys* anal*'):
        ret_value = 'v19'
    elif fnmatch.fnmatch(ls_title, '*sys* integ*'):
        ret_value = 'v20'
    elif find(ls_title, ['internet']):
        ret_value = 'v08'
    elif find(ls_title, ['computer op']) or find(ls_title, ['comp op']):
        ret_value = 'v02'
    elif find(ls_title, ['data entry']):
        ret_value = 'v03'
    elif find(ls_title, ['desktop publ']):
        ret_value = 'v05'
    elif find(ls_title, ['data proc']):
        ret_value = 'v04'
    elif find(ls_title, ['word proc']):
        ret_value = 'v25'
    elif find(ls_title, ['dp communication']):
        ret_value = 'v04'
    elif fnmatch.fnmatch(ls_title, '*dp info* sys*'):
        ret_value = 'v04'
    elif find(ls_title, ['dp communication']):
        ret_value = 'v04'
    elif re_find(ls_title, '*dp communication* equip*'):
        ret_value = 'v04'
    elif re_find(ls_title, '*dp communication* sys*'):
        ret_value = 'v04'
    elif find(ls_title, ['dp hardware']):
        ret_value = 'v04'
    elif re_find(ls_title, '*dp tech* support*'):
        ret_value = 'v04'
    elif find(ls_title, ['dp software']):
        ret_value = 'v04'
    elif find(ls_title, ['dp program']):
        ret_value = 'v04'
    elif find(ls_title, ['dp systems']):
        ret_value = 'v04'
    elif find(ls_title, ['programmer', 'software', 'prgmr']) or fnmatch.fnmatch(ls_title, 'sw *'):
        ret_value = 'v29'
    elif fnmatch.fnmatch(ls_title, '*info* sec*'):
        ret_value = 'v30'
    elif find(ls_title, ['network']):
        ret_value = 'v27'
    elif find(ls_title, ['database', ' db ', 'sql', 'oracle', 'sybase', ' dba ']) or \
            re_find_list(ls_title, ['dba *', 'db *', '* dba']) or ls_title == 'dba':
        ret_value = 'v28'
    elif re_find_list(ls_title, ['* comm', '* comm *', 'comm *']) or ls_title == 'comm' or find(
            ls_title, ['communcation']):
        ret_value = 'u01'
    elif find(ls_title, ['radio']):
        ret_value = 'u02'
    elif re_find_list(ls_title, ['* tv', '* tv *', 'tv *']) or find(ls_title, [
        'television']) or ls_title == 'tv':
        ret_value = 'u03'
    elif (find(ls_title, ['tele com']) or fnmatch.fnmatch(ls_title,
                                                          '*telecom*')) and not fnmatch.fnmatch(
        ls_title, '*commuter*'):
        ret_value = 'u04'
    elif find(ls_title, ['motion picture']) or find(ls_title, ['movie']):
        ret_value = 'u05'
    elif find(ls_title, ['audio', ' a v ', ' av ']) or re_find_list(ls_title, ['a v *', '* a v',
                                                                               'av *']) or fnmatch.fnmatch(
        ls_title, 'av') or fnmatch.fnmatch(ls_title, 'a v'):
        ret_value = 'u06'
    elif find(ls_title, ['broadcast']) or find(ls_title, ['broad cast']):
        ret_value = 'u07'
    elif find(ls_title, ['video']):
        ret_value = 'u09'
    elif find(ls_title, ['media']) and not find(ls_title, ['multi']):
        ret_value = 'u10'
    elif find(ls_title, ['serv ', 'service']) or fnmatch.fnmatch(
            ls_title, '* serv'):
        ret_value = 't01'
    elif find(ls_title, ['pub']) and fnmatch.fnmatch(ls_title, '*serv*') and not fnmatch.fnmatch(
            ls_title, '*publis*'):
        ret_value = 't04'
    elif find(ls_title, ['hospitality']) and find(ls_title, ['serv']):
        ret_value = 't03'
    elif find(ls_title, ['food']) and find(ls_title, ['serv']):
        ret_value = 't02'
    elif (find(ls_title, ['headq', 'head q'])) and (find(ls_title, ['intl', 'internat'])):
        ret_value = 's05'
    elif (find(ls_title, ['brnch', 'branch'])) and (find(ls_title, ['intl', 'internat'])):
        ret_value = 's04'
    elif find(ls_title, ['export']):
        ret_value = 's01'
    elif find(ls_title, ['import']):
        ret_value = 's02'
    elif find(ls_title, ['import']) and find(ls_title, ['export']):
        ret_value = 's03'
    elif find(ls_title, ['import']):
        ret_value = 's02'
    elif find(ls_title, ['educ']) or fnmatch.fnmatch(ls_title, '*lecture*') or fnmatch.fnmatch(
            ls_title, '*st grade*') or fnmatch.fnmatch(ls_title, '*nd grade*') or find(ls_title, [
        'rd grade']) or fnmatch.fnmatch(ls_title, '*th grade*') or fnmatch.fnmatch(ls_title,
                                                                                   '*academic*') or find(
        ls_title, ['edu ']) or fnmatch.fnmatch(ls_title, '*school*') or fnmatch.fnmatch(ls_title,
                                                                                        '*college*') or find(
        ls_title, ['university']):
        ret_value = 'r21'
    elif find(ls_title, ['librar']):
        ret_value = 'r03'
    elif find(ls_title, ['bursar']):
        ret_value = 'r20'
    elif find(ls_title, ['registrar']):
        ret_value = 'r19'
    elif ls_title == 'student' or fnmatch.fnmatch(ls_title, '* student') or fnmatch.fnmatch(
            ls_title, 'student of *'):
        ret_value = 'r16'
    elif (find(ls_title, ['edu']) and find(ls_title, ['consult'])) or (
            fnmatch.fnmatch(ls_title, 'ed *') and find(ls_title, ['consult'])) or (
            find(ls_title, [' ed ']) and find(ls_title, ['consult'])):
        ret_value = 'r14'
    elif find(ls_title, ['curriculum super']):
        ret_value = 'r13'
    elif (find(ls_title, ['dean of students'])) or (ls_title == 'dean') or (
            find(ls_title, ['dean'])):
        ret_value = 'r12'
    elif (find(ls_title, ['teacher']) or ls_title == 'tchr'):
        ret_value = 'r08'
    elif fnmatch.fnmatch(ls_title, '*teacher* aide*'):
        ret_value = 'r10'
    elif fnmatch.fnmatch(ls_title, '*sub* teacher*'):
        ret_value = 'r11'
    elif find(ls_title, ['guidance counsel']):
        ret_value = 'r02'
    elif find(ls_title, ['instructor']):
        ret_value = 'r07'
    elif find(ls_title, ['professor']) or find(ls_title, ['prof of ']):
        ret_value = 'r06'
    elif fnmatch.fnmatch(ls_title, '*contin* ed*') or fnmatch.fnmatch(ls_title, '*cont* edu*'):
        ret_value = 'r05'
    elif find(ls_title, ['superin']) and find(ls_title, ['scho']):
        ret_value = 'r22'
    elif find(ls_title, ['plann']):
        ret_value = 'q01'
    elif find(ls_title, ['financial pl', 'fin pl']):
        ret_value = 'q02'
    elif find(ls_title, ['site pl', 'facility pl']):
        ret_value = 'q03'
    elif find(ls_title, ['corp pl', 'corporate pl']):
        ret_value = 'q04'
    elif fnmatch.fnmatch(ls_title, '*material* pl*'):
        ret_value = 'q05'
    elif find(ls_title, ['meeting pl']) or find(ls_title, ['mtg pl']):
        ret_value = 'q06'
    elif find(ls_title, ['property']):
        ret_value = 'p05'
    elif find(ls_title, ['real estate']):
        ret_value = 'p04'
    elif find(ls_title, ['land dev']):
        ret_value = 'p03'
    elif fnmatch.fnmatch(ls_title, '*prop* m*') and not find(ls_title, ['propo']):
        ret_value = 'p02'
    elif find(ls_title, ['real estate broker']) or fnmatch.fnmatch(ls_title, '*real estate ag*'):
        ret_value = 'p01'
    elif ((find(ls_title, ['training', 'trainer', 'trg', 'trng']) or find(ls_title, ['trnr'])) and
          (not find(ls_title, ['athletic']))):
        ret_value = 'o06'
    elif find(ls_title, ['staff dev']):
        ret_value = 'o05'
    elif find(ls_title, ['mgmt dev', 'manager dev', 'management dev']):
        ret_value = 'o04'
    elif find(ls_title, ['exe dev', 'exec dev', 'executive dev']):
        ret_value = 'o03'
    elif re_find_list(ls_title, ['*corp* trg*', '*corp* train*']):
        ret_value = 'o02'
    elif fnmatch.fnmatch(ls_title, '*corp* ed*') and not fnmatch.fnmatch(ls_title,
                                                                         '*edi*'):
        ret_value = 'o01'
    elif ((re_find_list(ls_title, ['eng *', '* eng']) or find(ls_title, ['engr', ' eng ',
                                                                         'engineer']) or fnmatch.fnmatch(
        ls_title, 'eng')) and (
                  not find(ls_title, ['soft']) and not find(ls_title, ['sales']) and not find(
              ls_title, ['applica']) and not find(ls_title, ['net']) and not find(ls_title, [
              'sys']) and not find(ls_title, ['it']) and not fnmatch.fnmatch(ls_title,
                                                                             'sw *') or not find(
              ls_title, ['app']))):
        ret_value = 'n16'
    elif chk_in(ls_title, ['engineer', 'eng', 'professional engineer', 'engineering manager',
                           'senior engineer', 'principal engineer', 'mechanical engineer',
                           'process engineer', 'manufacturing engineer', 'staff engineer',
                           'sr engineer', 'industrial engineer']):
        ret_value = 'n16'


    # added value n03
    elif fnmatch.fnmatch(ls_title, '*elec* eng*'):
        ret_value = 'n03'

    # added value n17
    elif fnmatch.fnmatch(ls_title, '*app* eng*') or fnmatch.fnmatch(ls_title,
                                                                    'ap eng*') or fnmatch.fnmatch(
        ls_title,
        '* ap eng*'):
        ret_value = 'n17'

    # change made to n15
    elif fnmatch.fnmatch(ls_title, '*comp eng*') or fnmatch.fnmatch(ls_title, '*computer eng*'):
        ret_value = 'n15'

    # change made to n12
    elif fnmatch.fnmatch(ls_title, '*production eng*') or fnmatch.fnmatch(ls_title, '*prdtn eng*'):
        ret_value = 'n12'

    # change made to n10
    elif fnmatch.fnmatch(ls_title, '*manu* eng*') or fnmatch.fnmatch(ls_title, 'man eng*') or (
            fnmatch.fnmatch(ls_title, '*mfg*') and fnmatch.fnmatch(ls_title,
                                                                   '* eng*')) or fnmatch.fnmatch(
        ls_title, '* man eng*'):
        ret_value = 'n10'
    # change made to n09
    elif (fnmatch.fnmatch(ls_title, '*eng*')) and (
            find(ls_title, ['dev', 'design', 'dsgn', ' des ', 'dvlp']) or fnmatch.fnmatch(ls_title,
                                                                                          'des *')):
        ret_value = 'n09'
    elif fnmatch.fnmatch(ls_title, '*packag* eng*'):
        ret_value = 'n14'
    elif fnmatch.fnmatch(ls_title, '*ind* eng*') and not fnmatch.fnmatch(ls_title, '*window*'):
        ret_value = 'n13'
    elif fnmatch.fnmatch(ls_title, '*production eng*'):
        ret_value = 'n12'
    elif fnmatch.fnmatch(ls_title, '*power eng*'):
        ret_value = 'n11'
    elif fnmatch.fnmatch(ls_title, '*manu* eng*') or fnmatch.fnmatch(ls_title, 'man eng*'):
        ret_value = 'n10'
    elif (find(ls_title, ['eng'])) and (find(ls_title, ['dev', 'design', 'dsgn'])):
        ret_value = 'n09'
    elif fnmatch.fnmatch(ls_title, '*mine eng*') or fnmatch.fnmatch(ls_title, '*mining eng*'):
        ret_value = 'n08'
    elif fnmatch.fnmatch(ls_title, '*mech* eng*'):
        ret_value = 'n07'
    elif fnmatch.fnmatch(ls_title, '*cvl eng*') or fnmatch.fnmatch(ls_title, '*civil eng*'):
        ret_value = 'n06'
    elif fnmatch.fnmatch(ls_title, '*archtectural eng*'):
        ret_value = 'n05'
    elif fnmatch.fnmatch(ls_title, '*struc* eng*'):
        ret_value = 'n04'
    elif fnmatch.fnmatch(ls_title, '*chem* eng*'):
        ret_value = 'n02'
    elif fnmatch.fnmatch(ls_title, '*aerona* eng*'):
        ret_value = 'n01'
    elif fnmatch.fnmatch(ls_title, '*procur*'):
        ret_value = 'm03'
    elif find(ls_title, ['purchasing', 'purchaser', 'purch ', ' purch ']) or fnmatch.fnmatch(
            ls_title, '* purch'):
        ret_value = 'm02'
    elif find(ls_title, ['buyer', 'buying']):
        ret_value = 'm01'
    elif fnmatch.fnmatch(ls_title, '*cert* life* under*'):
        ret_value = 'k05'
    elif fnmatch.fnmatch(ls_title, '*risk m*'):
        ret_value = 'k04'
    elif find(ls_title, ['underwrit', 'under writ']):
        ret_value = 'k03'
    elif fnmatch.fnmatch(ls_title, '*actuar*'):
        ret_value = 'k02'
    elif fnmatch.fnmatch(ls_title, '*insur*'):
        ret_value = 'k01'
    elif find(ls_title,
              ['customer serv', 'customer rep', 'customer care', 'customer support', 'consumer rel',
               'cust serv', 'customer svc', 'customer rel', 'cust rep', 'crm']):
        ret_value = 'j01'
    elif find(ls_title, ['mktg', 'marketing', 'marketer']) or ls_title == 'cmo':
        ret_value = 'i01'
    elif fnmatch.fnmatch(ls_title, '*sales*'):
        ret_value = 'h05'
    elif fnmatch.fnmatch(ls_title, '*retail sale*'):
        ret_value = 'h06'
    elif fnmatch.fnmatch(ls_title, '*telemark*'):
        ret_value = 'h04'
    elif fnmatch.fnmatch(ls_title, '*sales rep*'):
        ret_value = 'h03'
    elif find(ls_title, ['account exe', 'acct exe', 'acct m', 'account m', 'account super',
                         'acct super']) or ls_title == 'ae':
        ret_value = 'h02'
    elif find(ls_title, ['salesman', 'sales man ', 'salesperson', 'sales woman']) or re_find(
            ls_title, '*sales man'):
        ret_value = 'h01'
    elif fnmatch.fnmatch(ls_title, '*legal*'):
        ret_value = 'g10'
    elif fnmatch.fnmatch(ls_title, '*compliance*'):
        ret_value = 'g09'
    elif fnmatch.fnmatch(ls_title, '*licensing*'):
        ret_value = 'g08'
    elif fnmatch.fnmatch(ls_title, '*trademark*') or fnmatch.fnmatch(ls_title, '*trade mark*'):
        ret_value = 'g07'
    elif fnmatch.fnmatch(ls_title, '*patent*'):
        ret_value = 'g06'
    elif fnmatch.fnmatch(ls_title, '*legal counsel*'):
        ret_value = 'g05'
    elif fnmatch.fnmatch(ls_title, '*paralegal*') or fnmatch.fnmatch(ls_title, '*para legal*'):
        ret_value = 'g04'
    elif fnmatch.fnmatch(ls_title, '*judge*'):
        ret_value = 'g03'
    elif fnmatch.fnmatch(ls_title, '*attorney*') or fnmatch.fnmatch(ls_title,
                                                                    '* atty *') or fnmatch.fnmatch(
        ls_title, '*lawyer*'):
        ret_value = 'g01'
    elif find(ls_title, ['fireman']) or re_find_list(ls_title, ['*fire man', 'fire man']):
        ret_value = 'f42'
    elif fnmatch.fnmatch(ls_title, '*police*'):
        ret_value = 'f41'
    elif fnmatch.fnmatch(ls_title, '*warrant officer*'):
        ret_value = 'f40'
    elif fnmatch.fnmatch(ls_title, '*warden*'):
        ret_value = 'f39'
    elif re_find_list(ls_title, ['*treasury* dep*',
                                 '*dep* of the treasury*']):
        ret_value = 'f38'
    elif re_find_list(ls_title, ['*trans* dep*', '*dep* of trans*']):
        ret_value = 'f37'
    elif fnmatch.fnmatch(ls_title, '*sheriff*'):
        ret_value = 'f34'
    elif fnmatch.fnmatch(ls_title, '*sergeant*'):
        ret_value = 'f33'
    elif fnmatch.fnmatch(ls_title, '*probation off*'):
        ret_value = 'f32'
    elif fnmatch.fnmatch(ls_title, '*police chief*') or fnmatch.fnmatch(ls_title,
                                                                        '*chief of police*'):
        ret_value = 'f30'
    elif fnmatch.fnmatch(ls_title, '*paramedic*') or fnmatch.fnmatch(ls_title,
                                                                     'emt') or fnmatch.fnmatch(
        ls_title, '* emt *') or fnmatch.fnmatch(ls_title, 'emt *') or fnmatch.fnmatch(ls_title,
                                                                                      '* emt') or fnmatch.fnmatch(
        ls_title, '*paramedic*') or fnmatch.fnmatch(ls_title, '*emer* med* tech*'):
        ret_value = 'f29'
    elif fnmatch.fnmatch(ls_title, '*marshall*'):
        ret_value = 'f25'
    elif fnmatch.fnmatch(ls_title, 'major'):
        ret_value = 'f23'
    elif find(ls_title, ['lieutenant']):
        ret_value = 'f21'
    elif find(ls_title, ['investigator']):
        ret_value = 'f18'
    elif find(ls_title, ['gov']):
        ret_value = 'f17'
    elif fnmatch.fnmatch(ls_title, 'general'):
        ret_value = 'f16'
    elif find(ls_title, ['fire chief']):
        ret_value = 'f15'
    elif fnmatch.fnmatch(ls_title, '*fed* gov*'):
        ret_value = 'f14'
    elif find(ls_title, ['state gov']):
        ret_value = 'f36'
    elif find(ls_title, ['ensign']):
        ret_value = 'f13'
    elif find(ls_title, ['corporal']):
        ret_value = 'f10'
    elif fnmatch.fnmatch(ls_title, '*commerce* dep*') or fnmatch.fnmatch(ls_title,
                                                                         '*dep* of commerce*'):
        ret_value = 'f09'
    elif fnmatch.fnmatch(ls_title, '*commander*'):
        ret_value = 'f08'
    elif fnmatch.fnmatch(ls_title, '*colonel*'):
        ret_value = 'f07'
    elif fnmatch.fnmatch(ls_title, '*capt*') and (
            not fnmatch.fnmatch(ls_title, '*job*') and not fnmatch.fnmatch(ls_title,
                                                                           '*line*') and not fnmatch.fnmatch(
        ls_title, '*training*')):
        ret_value = 'f06'
    elif fnmatch.fnmatch(ls_title, '*admiral*'):
        ret_value = 'f01'
    elif fnmatch.fnmatch(ls_title, '*justice dep*'):
        ret_value = 'f19'
    elif fnmatch.fnmatch(ls_title, '*agricu* dep*') or fnmatch.fnmatch(ls_title, '*dep* of agr*'):
        ret_value = 'f02'
    elif fnmatch.fnmatch(ls_title, '*state dep*'):
        ret_value = 'f35'
    elif fnmatch.fnmatch(ls_title, '*dep* of labor*') or fnmatch.fnmatch(ls_title, '*labor dep*'):
        ret_value = 'f20'
    elif fnmatch.fnmatch(ls_title, '*def* dep*') or fnmatch.fnmatch(ls_title,
                                                                    '*dep* of def*') or fnmatch.fnmatch(
        ls_title, 'dod'):
        ret_value = 'f11'
    elif fnmatch.fnmatch(ls_title, '*us marine*'):
        ret_value = 'f24'
    elif fnmatch.fnmatch(ls_title, '*air force*'):
        ret_value = 'f03'
    elif fnmatch.fnmatch(ls_title, '*navy*'):
        ret_value = 'f28'
    elif fnmatch.fnmatch(ls_title, '*army*'):
        ret_value = 'f05'
    elif fnmatch.fnmatch(ls_title, '*military*'):
        ret_value = 'f26'
    elif fnmatch.fnmatch(ls_title, '*local gov*') or fnmatch.fnmatch(ls_title, '*mayor*'):
        ret_value = 'f22'
    elif find(ls_title,
              ['medical', 'health', 'therapist', 'veterinar', 'nursing', 'nurse', 'physician',
               'psychiatrist', 'health', 'dental', 'dentist', 'pharmacist', 'pediatric', 'sugeon',
               'rehab', 'chriopract', 'surgery']) \
            or chk_in(ls_title, ['rn', 'md', 'dmd', 'dds']) or fnmatch.fnmatch(ls_title, 'med *'):
        ret_value = 'e16'
    elif fnmatch.fnmatch(ls_title, '*hospital*') and not fnmatch.fnmatch(ls_title, '*hospitality*'):
        ret_value = 'e39'
    elif find(ls_title, ['health depart']):
        ret_value = 'e38'
    elif find(ls_title, ['public health care']):
        ret_value = 'e37'
    elif find(ls_title, ['private health care']):
        ret_value = 'e36'
    elif find(ls_title, ['veterinarian']):
        ret_value = 'e35'
    elif fnmatch.fnmatch(ls_title, '*urolog'):
        ret_value = 'e34'
    elif find(ls_title, ['surgeon']):
        ret_value = 'e33'
    elif find(ls_title, ['radiolog']):
        ret_value = 'e32'
    elif find(ls_title, ['psychologist']):
        ret_value = 'e31'
    elif find(ls_title, ['psychiatrist']):
        ret_value = 'e31'
    elif find(ls_title, ['physician']):
        ret_value = 'e30'
    elif find(ls_title, ['periodontist']):
        ret_value = 'e29'
    elif find(ls_title, ['pediatrician']):
        ret_value = 'e28'
    elif find(ls_title, ['patholog']):
        ret_value = 'e27'
    elif find(ls_title, ['otorhinolaryngolog']):
        ret_value = 'e26'
    elif find(ls_title, ['otologist']):
        ret_value = 'e25'
    elif find(ls_title, ['osteopath']):
        ret_value = 'e24'
    elif find(ls_title, ['orthopedist']):
        ret_value = 'e23'
    elif find(ls_title, ['orthodontist']):
        ret_value = 'e22'
    elif find(ls_title, ['oral surg']):
        ret_value = 'e21'
    elif find(ls_title, ['obstetrician']):
        ret_value = 'e20'
    elif find(ls_title, ['nurse', 'nursing', ' rn ']) or fnmatch.fnmatch(ls_title,
                                                                         'rn') or fnmatch.fnmatch(
        ls_title, 'rn *') or fnmatch.fnmatch(ls_title, '* rn'):
        ret_value = 'e19'
    elif fnmatch.fnmatch(ls_title, '*neurolog*'):
        ret_value = 'e18'
    elif fnmatch.fnmatch(ls_title, '*nephrolog*'):
        ret_value = 'e17'
    elif fnmatch.fnmatch(ls_title, '*laryngolog*'):
        ret_value = 'e15'
    elif fnmatch.fnmatch(ls_title, '*internist*'):
        ret_value = 'e14'
    elif fnmatch.fnmatch(ls_title, '*hematolog*'):
        ret_value = 'e13'
    elif fnmatch.fnmatch(ls_title, '*head and neck*'):
        ret_value = 'e12'
    elif fnmatch.fnmatch(ls_title, '*gynecolog*'):
        ret_value = 'e11'
    elif fnmatch.fnmatch(ls_title, '*gerontolog*'):
        ret_value = 'e10'
    elif fnmatch.fnmatch(ls_title, '*general practitioner*'):
        ret_value = 'e09'
    elif fnmatch.fnmatch(ls_title, '*gastroenterolog*'):
        ret_value = 'e08'
    elif fnmatch.fnmatch(ls_title, '*family practitioner*'):
        ret_value = 'e07'
    elif fnmatch.fnmatch(ls_title, '*endodont*'):
        ret_value = 'e06'
    elif fnmatch.fnmatch(ls_title, '*dermatolog*'):
        ret_value = 'e05'
    elif fnmatch.fnmatch(ls_title, '*dentist*') or fnmatch.fnmatch(ls_title, 'dds'):
        ret_value = 'e04'
    elif find(ls_title, ['case worker']):
        ret_value = 'e03'
    elif find(ls_title, ['social work']):
        ret_value = 'e03'
    elif find(ls_title, ['anesthesiolog']):
        ret_value = 'e02'
    elif fnmatch.fnmatch(ls_title, '*allerg*') and not fnmatch.fnmatch(ls_title, '*metal*'):
        ret_value = 'e01'
    elif find(ls_title, ['construction']):
        ret_value = 'd02'
    elif find(ls_title, ['contractor']):
        ret_value = 'd03'
    elif find(ls_title, ['remodel']):
        ret_value = 'd07'
    elif fnmatch.fnmatch(ls_title, '*resid* build* construction*'):
        ret_value = 'd08'
    elif fnmatch.fnmatch(ls_title, '*commercial build* construction*'):
        ret_value = 'd01'
    elif fnmatch.fnmatch(ls_title, '*light build* construction*'):
        ret_value = 'd06'
    elif fnmatch.fnmatch(ls_title, '*heavy build* construction*'):
        ret_value = 'd05'
    elif find(ls_title, ['publish']):
        ret_value = 'c09'
    elif find(ls_title, ['illustrator']):
        ret_value = 'c05'
    elif find(ls_title, ['writer']):
        ret_value = 'c04'
    elif find(ls_title, ['reporter']):
        ret_value = 'c03'
    elif find(ls_title, ['editor', 'editing']):
        ret_value = 'c02'
    elif find(ls_title, ['publisher']):
        ret_value = 'c01'
    elif find(ls_title, ['magazine publish']):
        ret_value = 'c08'
    elif find(ls_title, ['newspaper publish']):
        ret_value = 'c07'
    elif find(ls_title, ['book publish']):
        ret_value = 'c06'
    elif find(ls_title, [' hr ', 'human resource']) or re_find_list(ls_title, ['hr *', '* hr']):
        ret_value = 'b15'
    elif fnmatch.fnmatch(ls_title, '*compensation*'):
        ret_value = 'b14'
    elif fnmatch.fnmatch(ls_title, '*payroll*'):
        ret_value = 'b13'
    elif fnmatch.fnmatch(ls_title, '*hr generalist*') or fnmatch.fnmatch(ls_title,
                                                                         '*human res* generalist*'):
        ret_value = 'b12'
    elif fnmatch.fnmatch(ls_title, 'personnel *') or fnmatch.fnmatch(ls_title, 'personnel'):
        ret_value = 'b11'
    elif find(ls_title, ['osha']) or find(ls_title,
                                          ['safety']):
        ret_value = 'b10'
    elif find(ls_title, ['disability']):
        ret_value = 'b09'
    elif find(ls_title, ['multicultural affairs', 'diversity']):
        ret_value = 'b08'
    elif find(ls_title, ['pension']):
        ret_value = 'b07'
    elif find(ls_title, ['labor relation']):
        ret_value = 'b06'
    elif find(ls_title, ['personnel relation']):
        ret_value = 'b05'
    elif fnmatch.fnmatch(ls_title, '*emp* benefit*'):
        ret_value = 'b04'
    elif find(ls_title, ['wage admin']) or find(ls_title,
                                                ['salary']):
        ret_value = 'b03'
    elif find(ls_title, ['affirmative action']):
        ret_value = 'b02'
    elif find(ls_title, ['employement', 'recruit']):
        ret_value = 'b01'
    elif find(ls_title, ['estimator']):
        ret_value = 'a35'
    elif find(ls_title, ['corp']) and find(ls_title,
                                           ['secretary']):
        ret_value = 'a14'
    elif find(ls_title, ['controller', 'comptroller', 'ctrlr', 'cntrlr']):
        ret_value = 'a13'
    elif find(ls_title, ['collector', 'collections']):
        ret_value = 'a12'
    elif find(ls_title, ['bond']) and not find(ls_title,
                                               ['bail']):
        ret_value = 'a10'
    elif find(ls_title, ['assessor']):
        ret_value = 'a04'
    elif find(ls_title, ['apprais']):
        ret_value = 'a03'
    elif find(ls_title, ['adjuster']):
        ret_value = 'a02'
    elif find(ls_title, ['accounting', 'accting']):
        ret_value = 'a26'
    elif find(ls_title, ['accountant']):
        ret_value = 'a01'
    elif find(ls_title, ['treas']) and not find(ls_title, ['treasury']):
        ret_value = 'a25'
    elif find(ls_title, ['econom']):
        ret_value = 'a24'
    elif find(ls_title, ['teller']) and not find(ls_title, ['fortune']):
        ret_value = 'a23'
    elif find(ls_title, ['tax']):
        ret_value = 'a22'
    elif find(ls_title, ['securities']) and (
            not find(ls_title, ['comp']) and not find(ls_title, ['net']) and not find(ls_title,
                                                                                      ['sys'])):
        ret_value = 'a19'
    elif find(ls_title, ['audit']):
        ret_value = 'a06'
    elif find(ls_title, ['auditor']):
        ret_value = 'a07'
    elif find(ls_title, ['bank']):
        ret_value = 'a09'
    elif find(ls_title, ['banker']):
        ret_value = 'a08'
    elif find(ls_title, ['invest']):
        ret_value = 'a17'
    elif find(ls_title, ['merger', 'acquisition']):
        ret_value = 'a18'
    elif find(ls_title, ['trusts']):
        ret_value = 'a25'
    elif find(ls_title, ['bond ', ' bond ']) or re_find(ls_title, '* bonds'):
        ret_value = 'a21'
    elif find(ls_title, ['stock']):
        ret_value = 'a20'
    elif find(ls_title, ['credit']):
        ret_value = 'a30'
    elif find(ls_title, ['cost account', 'cost acct']):
        ret_value = 'a29'
    elif re_find(ls_title, '*acct* receiv*') or find(ls_title, ['accounts receiv']):
        ret_value = 'a27'
    elif re_find_list(ls_title, ['*acct* pay*',
                                 '*accounts pay*']):
        ret_value = 'a28'
    elif find(ls_title, ['revenue']):
        ret_value = 'a34'
    elif re_find_list(ls_title, ['*econ* forecast*',
                                 '*demand forecast*']):
        ret_value = 'a33'
    elif find(ls_title, ['budget']):
        ret_value = 'a32'
    elif find(ls_title, [' fin ', 'financ', ' cfo ']) or \
            re_find_list(ls_title, ['fin *', '* fin', '*chief* fin* off*', '*chief* fin* off*',
                                    'cfo *']) or \
            chk_in(ls_title, ['cfo']):
        ret_value = 'a31'
    elif re_find_list(ls_title,
                      ['*cert* pub* acc*', 'cpa *', '* cpa *', '* cpa']) or ls_title == 'cpa':
        ret_value = 'a11'
    return ret_value.upper()
