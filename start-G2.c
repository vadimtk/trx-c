#include <my_global.h>
#include <mysql.h>
#include <my_getopt.h>
#include <sys/time.h>

static char *opt_host_name = NULL; /* server host (default=localhost) */
static unsigned int opt_procid = 0;

static struct my_option my_opts[] = /* option information structures */
{
        {"host", 'h', "Host to connect to",
                (uchar **) &opt_host_name, NULL, NULL,
                GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"procid", 'I', "process id",
                (uchar **) &opt_procid, NULL, NULL,
                GET_UINT, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        { NULL, 0, NULL, NULL, NULL, NULL, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0 }
};

static my_bool
get_one_option (int optid, const struct my_option *opt, char *argument)
{
        return (0);
}

void finish_with_error(MYSQL *con)
{
  fprintf(stderr, "%s\n", mysql_error(con));
  mysql_close(con);
  exit(1);        
}

static int
print_stmt_error (MYSQL_STMT *stmt, char *message)
{
        fprintf (stdout, "%s\n", message);
        if (stmt != NULL)
        {
                fprintf (stdout, "Error %u (%s): %s\n",
                                mysql_stmt_errno (stmt),
                                mysql_stmt_sqlstate (stmt),
                                mysql_stmt_error (stmt));
                return mysql_stmt_errno (stmt);
        }
        return 0;
}

#define NACCOUNTS 100
int main(int argc, char **argv)
{

  int opt_err;
  /* select statement */
  MYSQL_STMT *stmt;
  char *select1;
  // Bind variables
  MYSQL_BIND param[2], result[1];
  int selId1, selId2, selVal;
  my_bool is_null[1];

  /* select proc statement */
  MYSQL_STMT *stmt2;
  char *select2;
  // Bind variables
  MYSQL_BIND result2[1];
  int sel2AV;
  my_bool is_null2[1];

  /* INSERT statement */
  MYSQL_STMT *stmtUpdate;
  char *sqlUpdate;
  // Bind variables
  MYSQL_BIND paramUpdate[1];
  int updVal;
  
  
  /* UPDATE proc statement */
  MYSQL_STMT *stmtUpdate2;
  char *sqlUpdate2;
  // Bind variables
  MYSQL_BIND paramUpdate2[2];
  int updId, updBalance;

  if ((opt_err = handle_options (&argc, &argv, my_opts, get_one_option)))
        exit (opt_err);

  MYSQL *con = mysql_init(NULL);
  if (con == NULL)
  {
      fprintf(stderr, "mysql_init() failed\n");
      exit(1);
  }  
  
  if (mysql_real_connect(con, opt_host_name, "root", "test", 
          "__percona", 0, NULL, 0) == NULL) 
  {
      finish_with_error(con);
  } 

  /* SELECT stmp prep work */

  select1= "SELECT val FROM __b WHERE val >= ? AND val < ? FOR UPDATE";

  stmt = mysql_stmt_init(con);
  if (stmt == NULL) {
      finish_with_error(con);
  }

  if (mysql_stmt_prepare(stmt, select1, strlen(select1)) != 0) {
          print_stmt_error(stmt, "Could not prepare statement");
          return;
  }

  // Initialize the result column structures
  memset (param, 0, sizeof (param)); /* zero the structures */
  memset (result, 0, sizeof (result)); /* zero the structures */
  
// Init param structure
// Select
  param[0].buffer_type     = MYSQL_TYPE_LONG;
  param[0].buffer         = (void *) &selId1;
  param[0].is_unsigned    = 0;
  param[0].is_null         = 0;
  param[0].length         = 0;

  param[1].buffer_type     = MYSQL_TYPE_LONG;
  param[1].buffer         = (void *) &selId2;
  param[1].is_unsigned    = 0;
  param[1].is_null         = 0;
  param[1].length         = 0;

// Result
  result[0].buffer_type     = MYSQL_TYPE_LONG;
  result[0].buffer         = (void *) &selVal;
  result[0].is_unsigned    = 0;
  result[0].is_null         = &is_null[0];
  result[0].length         = 0;

  // Bind param structure to statement
  if (mysql_stmt_bind_param(stmt, param) != 0) {
          print_stmt_error(stmt, "Could not bind parameters");
          return;
  }

  // Bind result
  if (mysql_stmt_bind_result(stmt, result) != 0) {
          print_stmt_error(stmt, "Could not bind results");
          return;
  }

  /* SELECT sync prep work */

  select2= "SELECT FLOOR(AVG(val)) av FROM __proc";

  stmt2 = mysql_stmt_init(con);
  if (stmt2 == NULL) {
      finish_with_error(con);
  }

  if (mysql_stmt_prepare(stmt2, select2, strlen(select2)) != 0) {
          print_stmt_error(stmt2, "Could not prepare statement");
          return;
  }

  // Initialize the result column structures
  memset (result2, 0, sizeof (result2)); /* zero the structures */
  
// Init param structure

// Result
  result2[0].buffer_type     = MYSQL_TYPE_LONG;
  result2[0].buffer         = (void *) &sel2AV;
  result2[0].is_unsigned    = 0;
  result2[0].is_null         = &is_null2[0];
  result2[0].length         = 0;

  // Bind param structure to statement

  // Bind result
  if (mysql_stmt_bind_result(stmt2, result2) != 0) {
          print_stmt_error(stmt2, "Could not bind results");
          return;
  }

  /* INSERT stmp prep work */

  sqlUpdate= "INSERT INTO __b (val) VALUES (?)";
  stmtUpdate = mysql_stmt_init(con);
  if (stmtUpdate == NULL) {
      finish_with_error(con);
  }

  if (mysql_stmt_prepare(stmtUpdate, sqlUpdate, strlen(sqlUpdate)) != 0) {
          print_stmt_error(stmtUpdate, "Could not prepare statement");
          return;
  }

  // Initialize the result column structures
  memset (paramUpdate, 0, sizeof (paramUpdate)); /* zero the structures */
  
// Init param structure
// Update
  paramUpdate[0].buffer_type     = MYSQL_TYPE_LONG;
  paramUpdate[0].buffer         = (void *) &updVal;
  paramUpdate[0].is_unsigned    = 0;
  paramUpdate[0].is_null         = 0;
  paramUpdate[0].length         = 0;


  // Bind param structure to statement
  if (mysql_stmt_bind_param(stmtUpdate, paramUpdate) != 0) {
          print_stmt_error(stmtUpdate, "Could not bind parameters");
          return;
  }


// UPDATE proc statement 
  sqlUpdate2= "UPDATE __proc SET val=? WHERE procid = ?";
  stmtUpdate2 = mysql_stmt_init(con);
  if (stmtUpdate2 == NULL) {
      finish_with_error(con);
  }

  if (mysql_stmt_prepare(stmtUpdate2, sqlUpdate2, strlen(sqlUpdate2)) != 0) {
          print_stmt_error(stmtUpdate2, "Could not prepare statement");
          return;
  }

  // Initialize the result column structures
  memset (paramUpdate2, 0, sizeof (paramUpdate2)); /* zero the structures */
  
// Init param structure
// Update
  paramUpdate2[0].buffer_type     = MYSQL_TYPE_LONG;
  paramUpdate2[0].buffer         = (void *) &updBalance;
  paramUpdate2[0].is_unsigned    = 0;
  paramUpdate2[0].is_null         = 0;
  paramUpdate2[0].length         = 0;

  paramUpdate2[1].buffer_type     = MYSQL_TYPE_LONG;
  paramUpdate2[1].buffer         = (void *) &updId;
  paramUpdate2[1].is_unsigned    = 0;
  paramUpdate2[1].is_null         = 0;
  paramUpdate2[1].length         = 0;

  // Bind param structure to statement
  if (mysql_stmt_bind_param(stmtUpdate2, paramUpdate2) != 0) {
          print_stmt_error(stmtUpdate2, "Could not bind parameters");
          return;
  }


  int i;
  int fromVal;
  int toVal;
  int insVal;

  int errcode;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srand(tv.tv_usec * tv.tv_sec);
//  srand ( time(NULL) );
  MYSQL_RES *res;
  MYSQL_ROW row;

  for  (i=0;i<10000000;i+=100){

          updId = opt_procid;
          updBalance = i;

          if (mysql_stmt_execute(stmtUpdate2) != 0) {
                  errcode = print_stmt_error(stmtUpdate2, "Could not execute UPDATE2");
                  return;
          }

          do {
                  if (mysql_query (con, "SELECT FLOOR(AVG(val)) av FROM __proc") != 0) /* the statement failed */
                  {
                          finish_with_error(con);
                  }
                  res = mysql_use_result(con);
                  row = mysql_fetch_row(res);
                  sel2AV=atoi(row[0]);
                  mysql_free_result(res);

                  /*
                  if (mysql_stmt_execute(stmt2) != 0) {
                          errcode = print_stmt_error(stmt2, "Could not execute SELECT2");
                          return;
                  }

                  if (mysql_stmt_store_result(stmt2) != 0) {
                          print_stmt_error(stmt2, "Could not buffer result set");
                          return;
                  }
                  mysql_stmt_free_result(stmt2); */
                  //printf("got select %d, expecting %d\n", sel2AV, i);
          } while (i > sel2AV);

          mysql_query(con, "BEGIN");
          // we take ranges in 100 interval
          // and divide into two parts
          // SELECT from one part and if no records found 
          // then INSERT into another part
          

          // After begin we sync with other processes, to make sure
          // there is enough concurrency

          int part = rand() % 2;

          if (part==0) {
                  fromVal =i;
                  toVal   =i+50;
                  insVal  =rand()%50+i+50;
          }else{
                  fromVal =i+50;
                  toVal   =i+100;
                  insVal  =rand()%50+i;
          }

          selId1 = fromVal;
          selId2 = toVal;

          if (mysql_stmt_execute(stmt) != 0) {
                  errcode = print_stmt_error(stmt, "Could not execute SELECT1");
                  if (errcode=1317) mysql_rollback(con);
                  continue;
          }

          if (mysql_stmt_store_result(stmt) != 0) {
                  print_stmt_error(stmt, "Could not buffer result set");
                  return;
          }
          int performInsert = 1;
          // if there are rows - we do not do INSERT
          if (mysql_stmt_num_rows(stmt)>0) {
                performInsert = 0;
          }
          mysql_stmt_free_result(stmt);

          // make transfer
          if (performInsert == 1){

                  updVal = insVal;
                  if (mysql_stmt_execute(stmtUpdate) != 0) {
                          errcode = print_stmt_error(stmtUpdate, "Could not execute INSERT");
                          if (errcode=1317) mysql_rollback(con);
                          continue;
                  }

          }
          printf("Trx from: %d to: %d, insert %d, do insert: %d\n",
                          fromVal, toVal, insVal, performInsert);
          if (mysql_commit(con)!=0){
                  fprintf(stdout, "COMMIT err: %s\n", mysql_error(con));
          }else{
                  fprintf(stdout, "COMMIT OK\n");
          }

  }

  mysql_stmt_close(stmt);

  mysql_close(con);
  
  exit(0);
}
