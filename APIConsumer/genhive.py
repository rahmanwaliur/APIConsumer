#from django.template import Context, loader
#from django.contrib.auth import get_user_model
#UserModel = get_user_model()

#from string import Template

def hivequery(user, cohort, medcodes):
    return "select country, gni from hdi where gni<2000"

"""
    #table = "%s.combids" % (user)
    table = "%s_combids" % (user)

    #print table
    # query = "USE THIN1205. DROP TABLE IF EXISTS %s. CREATE TABLE %s AS SELECT DISTINCT combid FROM medical WHERE medcode IN (" % (table, table)"

    code_list = {}

    if not medcodes:
        return False

    medcodes.sort()
    #print medcodes
    last_fc = medcodes[0][0]
    #print last_fc
    code_list[last_fc] = []
    #print code_list[last_fc]
    for code in medcodes:
        if code[0] == last_fc:
            code_list[last_fc].append("medcode LIKE '%s__'" % code)
        else:
            last_fc = code[0]
            code_list[last_fc] = ["medcode LIKE '%s__'" % code]
    #print code_list
    master_list = ''
    for key,value in code_list.iteritems():
        code_list[key] = "(medcode1 = '%s' AND (%s))" % (key, " OR ".join(value))
    master_list = " OR ".join(code_list.values())
    #print master_list
    hive_tables = ['demography_norm', 'ahd_denorm', 'medical_denorm', 'pvi_norm', 'therapy_norm', 'consult_denorm', 'patient_norm']

    thin_tables = ['demography', 'ahd', 'medical', 'pvi', 'therapy', 'consult', 'patient']

    create_db = "CREATE DATABASE IF NOT EXISTS %s" % user
    working_db = "USE default"
    drop_table1 = "DROP TABLE IF EXISTS %s" % table
    create_table1 = "CREATE TABLE IF NOT EXISTS %s (combid STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" % table
    testing_codes = "INSERT OVERWRITE TABLE %s SELECT DISTINCT combid FROM medical_norm WHERE %s" % (table,master_list)
    drop_table2 = "DROP TABLE IF EXISTS %s_%s" % (user,hive_tables)
    create_table12 = "CRETAE TABLE %s_%s ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS SELECT * FROM %s LEFT SEMI JOIN %s ON (combids.combid = %s.combid)" % (user, hive_tables, thin_tables, table, thin_tables)

    #query = "select * from diabdrug.medical where combid = %s" % master_list
    return {'createdb':create_db, 'workdb':working_db, 'droptable1': drop_table1, 'createtable1': create_table1, 'testcode':testing_codes, 'droptable2':drop_table2, 'createtable2':create_table12}
    #mode = "set hive.mapred.mode=nonstrict;"

    #context = Context({'tables': zip(thin_tables, hive_tables), 'table': table, 'codes': master_list, 'username': user, 'search': cohort.replace(" ", "_")})
    #t = loader.get_template('sql.txt')
    #return t.render(context)


# code for hive query generation

  #print medcodes


"""
