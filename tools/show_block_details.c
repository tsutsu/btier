#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<unistd.h>
#include<stdarg.h>
#include<errno.h>
#include<libgen.h>

#define _LARGEFILE64_SOURCE
#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#define RET_SYSFS -5
#define RET_SYSERR -2
#define RET_USAGE -1
#define RET_OK 0

#define die_syserr() { fprintf(stderr,"Fatal system error : %s",strerror(errno)); exit(RET_SYSERR); }

void usage(char *progname)
{
   fprintf(stderr,"Usage %s sdtier[N]\n",progname);
   exit(-1);
}

void *s_malloc(size_t size)
{
        void *retval;
        retval = malloc(size);
        if (!retval)
                die_syserr();
        return retval;
}

void *s_realloc(void *ptr, size_t size)
{
        void *retval;
        retval = realloc(ptr, size);

        if (!retval)
                die_syserr();
        return retval;
}

void *as_sprintf(const char *fmt, ...)
{
        /* Guess we need no more than 100 bytes. */
        int n, size = 100;
        void *p;
        va_list ap;
        p = s_malloc(size);
        while (1) {
                /* Try to print in the allocated space. */
                va_start(ap, fmt);
                n = vsnprintf(p, size, fmt, ap);
                va_end(ap);
                /* If that worked, return the string. */
                if (n > -1 && n < size)
                        return p;
                /* Else try again with more space. */
                if (n > -1)     /* glibc 2.1 */
                        size = n + 1;   /* precisely what is needed */
                else            /* glibc 2.0 */
                        size *= 2;      /* twice the old size */
                p = s_realloc(p, size);
        }
}

void get_blockinfo(char *device)
{
   FILE *fp;
   char *sysfs, *api;
   unsigned long long blocknr, total_blocks;
   char buf[128];

   sysfs=as_sprintf("/sys/block/%s/tier/size_in_blocks",device);
   api=as_sprintf("/sys/block/%s/tier/show_blockinfo",device);
   fp=fopen(sysfs,"r");
   if ( NULL == fp ) die_syserr();
   if(!fgets(buf,sizeof(buf)-1,fp)) die_syserr();
   if ( 1 != sscanf(buf,"%llu",&total_blocks)) exit(RET_SYSFS);
   fclose(fp);  
   fp=fopen(api,"w+");
   if ( NULL == fp ) die_syserr();
   for ( blocknr=0; blocknr<total_blocks;blocknr++) {
       fprintf(fp,"%llu\r\n",blocknr); 
       fflush(fp);
       rewind(fp);
       memset(buf,0,sizeof(buf));
       if ( !fgets(buf,sizeof(buf)-1,fp)) die_syserr();
       printf("%llu %s",blocknr,buf);      
   }
   fclose(fp);
   free(sysfs); 
   free(api); 
}

int main (int argc, char *argv[])
{
   char *device;
   char *fullpath;
   FILE *fp;
   struct stat stbuf;

   if ( argc <= 1 ) 
        usage(argv[0]); 

   if ( 0 != strncmp(argv[1],"sdtier", strlen("sdtier")))
        usage(argv[0]);
  
   fullpath=as_sprintf("/dev/%s",argv[1]);

   if ( 0 != stat(fullpath,&stbuf)) {
      fprintf(stderr,"No such device\n");
      exit(-1);
   }
   get_blockinfo(argv[1]);
   free(fullpath); 
   exit(0); 
}
