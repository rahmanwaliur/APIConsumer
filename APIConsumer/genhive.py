from django.template import Context, loader
from django.contrib.auth import get_user_model
UserModel = get_user_model()



def hivequery(user, cohort, medcodes):

    table = "%s.combids" % (user)

    # query = "USE THIN1205. DROP TABLE IF EXISTS %s. CREATE TABLE %s AS SELECT DISTINCT combid FROM medical WHERE medcode IN (" % (table, table)"

    code_list = {}

    if not medcodes:
        return False

    medcodes.sort()

    last_fc = medcodes[0][0]
    code_list[last_fc] = []

    for code in medcodes:
        if code[0] == last_fc:
            code_list[last_fc].append("medcode LIKE '%s__'" % code)
        else:
            last_fc = code[0]
            code_list[last_fc] = ["medcode LIKE '%s__'" % code]

    master_list = ''
    for key,value in code_list.iteritems():
        code_list[key] = "(medcode1 = '%s' AND (%s))" % (key, " OR ".join(value))
    master_list = " OR ".join(code_list.values())
    hive_tables = ['demography_norm', 'ahd_denorm', 'medical_denorm', 'pvi_norm', 'therapy_norm', 'consult_denorm', 'patient_norm']
    thin_tables = ['demography', 'ahd', 'medical', 'pvi', 'therapy', 'consult', 'patient']
    context = Context({'tables': zip(thin_tables, hive_tables), 'table': table, 'codes': master_list, 'username': user, 'search': cohort.replace(" ", "_")})
    t = loader.get_template('sql.txt')
    return t.render(context)
# code for hive query generation

  #print medcodes

  #return "select country, gni from hdi where gni<2000"


