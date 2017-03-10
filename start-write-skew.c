#include <my_global.h>
#include <mysql.h>
#include <my_getopt.h>

#include <sys/time.h>
#include <pthread.h>
#include <string.h>

static char *opt_host1_name = NULL; /* server host (default=localhost) */
static char *opt_host2_name = NULL; /* server host (default=localhost) */
static unsigned int opt_procid = 0;
static unsigned int opt_threads = 0;

static struct my_option my_opts[] = /* option information structures */
{
        {"host1", 'h', "Host1 to connect to",
                (uchar **) &opt_host1_name, NULL, NULL,
                GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"host2", 'j', "Host2 to connect to",
                (uchar **) &opt_host2_name, NULL, NULL,
                GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"procid", 'I', "process id",
                (uchar **) &opt_procid, NULL, NULL,
                GET_UINT, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"threads", 'T', "number of threads",
                (uchar **) &opt_threads, NULL, NULL,
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
static int
print_conn_error (MYSQL *con, char *message)
{
        fprintf (stdout, "%s\n", message);
        if (con != NULL)
        {
                fprintf (stdout, "Error %u (%s): %s\n",
                                mysql_errno (con),
                                mysql_sqlstate (con),
                                mysql_error (con));
                return mysql_errno (con);
        }
        return 0;
}

#define NACCOUNTS 100
#define MAX_THREADS 1000

typedef struct _thread_data_t {
  int tid;
  char *mysql_host;
} thread_data_t;

void *worker(void *ptr) {

  thread_data_t *data = (thread_data_t *)ptr;
  int myid = data->tid;

  MYSQL *con = mysql_init(NULL);
  if (con == NULL)
  {
      fprintf(stderr, "mysql_init() failed\n");
      exit(1);
  }  

  if (mysql_real_connect(con, data->mysql_host, "root", "test", 
          "__percona", 0, NULL, 0) == NULL) 
  {
      finish_with_error(con);
  } 

  int i;

  MYSQL_RES *res;
  MYSQL_ROW row;

  for  (i=0; i<1000000; i++){
  // check invariant. we must have SUM <=1
          int sel2AV;

          if (mysql_query (con, "SELECT SUM(val) av FROM __c") != 0) /* the statement failed */
          {
                  finish_with_error(con);
          }

          res = mysql_use_result(con);
          row = mysql_fetch_row(res);
          sel2AV=atoi(row[0]);
          mysql_free_result(res);
          
          if (sel2AV > 1) {
                  printf("Constraint failed, exiting. SUM: %d\n", sel2AV);
                  exit(1);
          }

  // if check passed - reset counter for the thread
          char query[1000];
          sprintf(query,  "UPDATE __c SET val = 0 WHERE id=%d", myid);
          if (mysql_query (con, query) != 0)
          {
                  finish_with_error(con);
          }

  // now we wait to get all thread in sync
          fprintf(stdout,"Thread %d, waiting on sync\n",myid);

          do {
                  if (mysql_query (con, "SELECT SUM(val) av FROM __c") != 0) /* the statement failed */
                  {
                          finish_with_error(con);
                  }

                  res = mysql_use_result(con);
                  row = mysql_fetch_row(res);
                  sel2AV=atoi(row[0]);
                  mysql_free_result(res);

          } while (sel2AV > 0);

  // execute main transaction:
          if (mysql_query(con, "BEGIN")!=0)
          {
                  finish_with_error(con);
          }

          sprintf(query,  "UPDATE __c SET val = 1 WHERE id=%d", myid);
          int errcode;
          if (mysql_query (con, query) != 0)
          {
                  errcode = print_conn_error(con, "Could not execute UPDATE");
                  if (errcode=1317) mysql_rollback(con);
                  continue;
          }

          if (mysql_query (con, "SELECT SUM(val) av FROM __c FOR UPDATE") != 0) /* the statement failed */
          {
                  errcode = print_conn_error(con, "Could not execute SELECT");
                  if (errcode=1317) mysql_rollback(con);
                  continue;
          } 
          res = mysql_use_result(con);
           
          errcode=0;
          if (res) {
                  if (row = mysql_fetch_row(res)) {
                          sel2AV=atoi(row[0]);
                  }else {
                         errcode = print_conn_error(con, "Could not fetch results on SELECT");
                  }
                  mysql_free_result(res);
                  if (errcode) continue; // if we got 1213 on fetching results
          } else {
                  errcode = print_conn_error(con, "Could not get results on SELECT");
          }
          if (sel2AV > 1) {
                  printf("T: %d, Constraint failed, rolling back. SUM: %d\n", myid, sel2AV);
                  mysql_rollback(con);
                  continue;
          }
        // commiting 
          if (mysql_commit(con)!=0){
                  fprintf(stdout, "T: %d, COMMIT err: %s\n", myid, mysql_error(con));
          }else{
                  fprintf(stdout, "T: %d, COMMIT OK\n", myid);
          }

 }


 mysql_close(con);
  pthread_exit(0);
}


int main(int argc, char **argv)
{
  mysql_library_init(0,NULL,NULL);
  pthread_t workers[MAX_THREADS];
  thread_data_t thr_data[MAX_THREADS];
  char query[1000];

  int opt_err;


  if ((opt_err = handle_options (&argc, &argv, my_opts, get_one_option)))
        exit (opt_err);

/* do a prep work */
  MYSQL *con = mysql_init(NULL);
  if (con == NULL)
  {
      fprintf(stderr, "mysql_init() failed\n");
      exit(1);
  }  

  if (mysql_real_connect(con, opt_host1_name, "root", "test", 
          "__percona", 0, NULL, 0) == NULL) 
  {
          finish_with_error(con);
  } 

  if (mysql_query (con, "DROP TABLE IF EXISTS __c") != 0)
  {
          finish_with_error(con);
  }
  if (mysql_query (con, "CREATE TABLE __c (id int primary key, val int)") != 0)
  {
          finish_with_error(con);
  }

  int i;

  for (i = 0; i < opt_threads; i++) {
          sprintf(query,  "INSERT INTO __c VALUES (%d, 0)", i);
          if (mysql_query (con, query) != 0)
          {
                  finish_with_error(con);
          }
  }


  for (i = 0; i < opt_threads; i++) {
        if (i%2 == 0) thr_data[i].mysql_host=opt_host1_name;
        else thr_data[i].mysql_host=opt_host2_name;
        thr_data[i].tid=i;

        pthread_create(&workers[i], NULL, worker, &thr_data[i]);
  }

  for (i = 0; i < opt_threads; i++) {
          pthread_join(workers[i], NULL);
  }
  
  exit(0);
}
