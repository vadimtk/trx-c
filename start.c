#include <my_global.h>
#include <mysql.h>
#include <my_getopt.h>

static char *opt_host_name = NULL; /* server host (default=localhost) */

static struct my_option my_opts[] = /* option information structures */
{
        {"host", 'h', "Host to connect to",
                (uchar **) &opt_host_name, NULL, NULL,
                GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
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
  MYSQL_BIND param[1], result[1];
  int selId, selBalance;
  my_bool is_null[1];

  /* UPDATE statement */
  MYSQL_STMT *stmtUpdate;
  char *sqlUpdate;
  // Bind variables
  MYSQL_BIND paramUpdate[2];
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

  select1= "SELECT balance FROM __a WHERE id = ? FOR UPDATE";
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
  param[0].buffer         = (void *) &selId;
  param[0].is_unsigned    = 0;
  param[0].is_null         = 0;
  param[0].length         = 0;

// Result
  result[0].buffer_type     = MYSQL_TYPE_LONG;
  result[0].buffer         = (void *) &selBalance;
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

  /* SELECT stmp prep work */

  sqlUpdate= "UPDATE __a SET balance=? WHERE id = ?";
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
  paramUpdate[0].buffer         = (void *) &updBalance;
  paramUpdate[0].is_unsigned    = 0;
  paramUpdate[0].is_null         = 0;
  paramUpdate[0].length         = 0;

  paramUpdate[1].buffer_type     = MYSQL_TYPE_LONG;
  paramUpdate[1].buffer         = (void *) &updId;
  paramUpdate[1].is_unsigned    = 0;
  paramUpdate[1].is_null         = 0;
  paramUpdate[1].length         = 0;

  // Bind param structure to statement
  if (mysql_stmt_bind_param(stmtUpdate, paramUpdate) != 0) {
          print_stmt_error(stmtUpdate, "Could not bind parameters");
          return;
  }

  int i;
  int fromAccnt;
  int toAccnt;
  int fromBalance;
  int toBalance;

  int errcode;

  for  (i=0;i<1000000;i++){

          // handle fromAccount
          mysql_query(con, "BEGIN");

          fromAccnt = rand() % NACCOUNTS;

          selId = fromAccnt;

          if (mysql_stmt_execute(stmt) != 0) {
                  errcode = print_stmt_error(stmt, "Could not execute SELECT1");
                  if (errcode=1317) mysql_rollback(con);
                  continue;
          }

          if (mysql_stmt_store_result(stmt) != 0) {
                  print_stmt_error(stmt, "Could not buffer result set");
                  return;
          }

          if(mysql_stmt_fetch (stmt) == 0){
                  fromBalance = selBalance;
          }
          mysql_stmt_free_result(stmt);

          // handle toAccount
          do {
                  toAccnt = rand() % NACCOUNTS;
          } while (toAccnt == fromAccnt);


          selId = toAccnt;

          if (mysql_stmt_execute(stmt) != 0) {
                  errcode = print_stmt_error(stmt, "Could not execute SELECT2");
                  if (errcode=1317) mysql_rollback(con);
                  continue;
          }

          if (mysql_stmt_store_result(stmt) != 0) {
                  print_stmt_error(stmt, "Could not buffer result set");
                  return;
          }

          if(mysql_stmt_fetch (stmt) == 0){
                  toBalance = selBalance;
          }
          mysql_stmt_free_result(stmt);

          // make transfer
          if (fromBalance > 1){
                  int moveAmnt= rand()%(fromBalance/2)+1;
                  int fromAmnt=fromBalance - moveAmnt;
                  int toAmnt =toBalance + moveAmnt;

                  updId = fromAccnt;
                  updBalance = fromAmnt;
                  if (mysql_stmt_execute(stmtUpdate) != 0) {
                          errcode = print_stmt_error(stmtUpdate, "Could not execute UPDATE1");
                          if (errcode=1317) mysql_rollback(con);
                          continue;
                  }
                  updId = toAccnt;
                  updBalance = toAmnt;
                  if (mysql_stmt_execute(stmtUpdate) != 0) {
                          errcode = print_stmt_error(stmtUpdate, "Could not execute UPDATE2");
                          if (errcode=1317) mysql_rollback(con);
                          continue;
                  }
                printf("Update %d from account %d, balance %d -> to account %d, balance %d\n",
                          moveAmnt, fromAccnt, fromBalance, toAccnt, toBalance);

          }
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
