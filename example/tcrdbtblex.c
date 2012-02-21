#include <tcrdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

int main(int argc, char **argv){

  TCRDB *rdb;
  int ecode, pksiz, i, rsiz;
  char pkbuf[256];
  const char *rbuf, *name;
  TCMAP *cols;
  RDBQRY *qry;
  TCLIST *res;

  /* create the object */
  rdb = tcrdbnew();

  /* connect to the server */
  if(!tcrdbopen(rdb, "localhost", 1978)){
    ecode = tcrdbecode(rdb);
    fprintf(stderr, "open error: %s\n", tcrdberrmsg(ecode));
  }

  /* store a record */
  pksiz = sprintf(pkbuf, "%ld", (long)tcrdbtblgenuid(rdb));
  cols = tcmapnew3("name", "mikio", "age", "30", "lang", "ja,en,c", NULL);
  if(!tcrdbtblput(rdb, pkbuf, pksiz, cols)){
    ecode = tcrdbecode(rdb);
    fprintf(stderr, "put error: %s\n", tcrdberrmsg(ecode));
  }
  tcmapdel(cols);

  /* store a record in a naive way */
  pksiz = sprintf(pkbuf, "12345");
  cols = tcmapnew();
  tcmapput2(cols, "name", "falcon");
  tcmapput2(cols, "age", "31");
  tcmapput2(cols, "lang", "ja");
  if(!tcrdbtblput(rdb, pkbuf, pksiz, cols)){
    ecode = tcrdbecode(rdb);
    fprintf(stderr, "put error: %s\n", tcrdberrmsg(ecode));
  }
  tcmapdel(cols);

  /* search for records */
  qry = tcrdbqrynew(rdb);
  tcrdbqryaddcond(qry, "age", RDBQCNUMGE, "20");
  tcrdbqryaddcond(qry, "lang", RDBQCSTROR, "ja,en");
  tcrdbqrysetorder(qry, "name", RDBQOSTRASC);
  tcrdbqrysetlimit(qry, 10, 0);
  res = tcrdbqrysearch(qry);
  for(i = 0; i < tclistnum(res); i++){
    rbuf = tclistval(res, i, &rsiz);
    cols = tcrdbtblget(rdb, rbuf, rsiz);
    if(cols){
      printf("%s", rbuf);
      tcmapiterinit(cols);
      while((name = tcmapiternext2(cols)) != NULL){
        printf("\t%s\t%s", name, tcmapget2(cols, name));
      }
      printf("\n");
      tcmapdel(cols);
    }
  }
  tclistdel(res);
  tcrdbqrydel(qry);

  /* close the connection */
  if(!tcrdbclose(rdb)){
    ecode = tcrdbecode(rdb);
    fprintf(stderr, "close error: %s\n", tcrdberrmsg(ecode));
  }

  /* delete the object */
  tcrdbdel(rdb);

  return 0;
}
