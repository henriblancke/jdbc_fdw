/*
 * --------------------------------------------- jq.c Implementation of Low
 * level JDBC based functions replacing the libpq-fe functions
 *
 * Heimir Sverrisson, 2015-04-13
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * ---------------------------------------------
 */
#include <stdlib.h>
#include "postgres.h"
#include "jdbc_fdw.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "commands/defrem.h"
#include "libpq-fe.h"

#include "jni.h"

#define Str(arg) #arg
#define StrValue(arg) Str(arg)
#define STR_SHAREEXTDIR StrValue(SHARE_EXT_DIR)
/* Number of days from unix epoch time (1970-01-01) to postgres epoch time (2000-01-01) */
#define POSTGRES_TO_UNIX_EPOCH_DAYS 		(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
/* POSTGRES_TO_UNIX_EPOCH_DAYS to microseconds */
#define POSTGRES_TO_UNIX_EPOCH_USECS 		(POSTGRES_TO_UNIX_EPOCH_DAYS * USECS_PER_DAY)

#ifdef _WIN32
#define PATH_SEPARATOR ";"
#else
#define PATH_SEPARATOR ":"
#endif

/*
 * Local housekeeping functions and Java objects
 */

/* JVM variables - PostgreSQL backends are single-threaded, no need for thread-local */
static JNIEnv *Jenv = NULL;
static JavaVM *jvm = NULL;
static bool jvm_initialized = false;

/* Session tracking for pgBouncer detection */
static Oid last_userid = InvalidOid;
static Oid last_dbid = InvalidOid;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct jdbcFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Structure holding options from the foreign server and user mapping
 * definitions
 */
typedef struct JserverOptions
{
	char	   *url;
	char	   *drivername;
	char	   *username;
	char	   *password;
	int			querytimeout;
	char	   *jarfile;
	int			maxheapsize;
}			JserverOptions;

static JserverOptions opts;

/* Local function prototypes */
static int	jdbc_connect_db_complete(JDBCUtilsInfo * jdbcUtilsInfo);
void		jdbc_jvm_init(const ForeignServer *server, const UserMapping *user);
static void jdbc_get_server_options(JserverOptions * opts, const ForeignServer *f_server, const UserMapping *f_mapping);
static JDBCUtilsInfo * jdbc_create_JDBC_connection(const ForeignServer *server, const UserMapping *user);
/*
 * Uses a String object's content to create an instance of C String
 */
static char *jdbc_convert_string_to_cstring(jobject);

/*
 * Convert byte array to Datum
 */
static Datum jdbc_convert_byte_array_to_datum(jbyteArray);

/*
 * Common function to convert Object value to datum
 */
static Datum jdbc_convert_object_to_datum(Oid, int32, jobject);

/*
 * JVM destroy function
 */
static void jdbc_destroy_jvm();

/*
 * JVM detach function - currently unused as we keep JVM attached for backend lifetime
 * static void jdbc_detach_jvm();
 */

/*
 * Connection cache reset function for pgBouncer support
 */
void jdbc_reset_connection_cache(void);

/*
 * clears any exception that is currently being thrown
 */
void		jq_exception_clear(void);

/*
 * check for pending exceptions
 */
void		jq_get_exception(void);

/*
 * get table infomations for importForeignSchema
 */
static List *jq_get_column_infos(JDBCUtilsInfo * jdbcUtilsInfo, char *tablename);
static List *jq_get_table_names(JDBCUtilsInfo * jdbcUtilsInfo, const char *schemaPattern, const char *tableNames);


static void jq_get_JDBCUtils(JDBCUtilsInfo * jdbcUtilsInfo, jclass * JDBCUtilsClass, jobject * JDBCUtilsObject);

/*
 * jq_release_jdbc_utils_object
 *		Clean up and delete the global JNI reference for a JDBCUtilsObject
 */
void
jq_release_jdbc_utils_object(JDBCUtilsInfo * jdbcUtilsInfo)
{
	jobject		utils_obj;

	/*
	 * CRITICAL: Read JDBCUtilsObject ONCE and use that value throughout!
	 * Multiple reads cause corruption. This function may be called during
	 * error handling, so we must NOT trigger any errors here.
	 */
	if (jdbcUtilsInfo == NULL)
		return;

	if (jvm == NULL || Jenv == NULL)
		return;

	/* Read the field ONCE into a local variable */
	utils_obj = jdbcUtilsInfo->JDBCUtilsObject;

	if (utils_obj == NULL)
		return;

	/*
	 * CRITICAL SAFETY: DO NOT delete global references during cleanup!
	 *
	 * Deleting global JNI references during PostgreSQL cleanup/shutdown
	 * causes JVM crashes (SIGSEGV, SIGABRT). The JVM may be in an unstable
	 * state during cleanup, and DeleteGlobalRef can trigger internal JVM
	 * errors.
	 *
	 * Since this function is called during transaction abort, backend exit,
	 * or cache invalidation - times when the JVM/backend may be shutting down -
	 * it's safer to just mark the reference as NULL and let the JVM clean up
	 * its own resources on shutdown.
	 *
	 * This may cause minor memory leaks in long-running backends that create
	 * and destroy many connections, but it prevents crashes which are worse.
	 */

	/* Just mark as cleaned up, don't actually delete anything */
	jdbcUtilsInfo->JDBCUtilsObject = NULL;
}

/*
 * JNI Resource Management Helpers
 *
 * These functions ensure proper cleanup of JNI resources even when
 * PostgreSQL's ereport(ERROR) is called.
 */

typedef struct JNIFrameGuard
{
	bool frame_pushed;
	JNIEnv *env;
} JNIFrameGuard;

/*
 * jni_push_frame - Push a local frame with error handling
 *
 * Returns: JNIFrameGuard that must be passed to jni_pop_frame
 */
static JNIFrameGuard
jni_push_frame(int capacity)
{
	JNIFrameGuard guard = {false, Jenv};

	if (Jenv == NULL)
		ereport(ERROR, (errmsg("JNI environment not initialized")));

	if ((*Jenv)->PushLocalFrame(Jenv, capacity) < 0)
		ereport(ERROR, (errmsg("Failed to push JNI local frame - out of memory")));

	guard.frame_pushed = true;
	return guard;
}

/*
 * jni_pop_frame - Pop a local frame, optionally preserving a result
 *
 * result: jobject to preserve (can be NULL)
 * Returns: The preserved result (or NULL)
 */
static jobject
jni_pop_frame(JNIFrameGuard *guard, jobject result)
{
	jobject preserved = NULL;

	if (guard->frame_pushed && guard->env != NULL)
	{
		/* PopLocalFrame preserves the result reference */
		preserved = (*guard->env)->PopLocalFrame(guard->env, result);
		guard->frame_pushed = false;
	}

	return preserved;
}

/*
 * jni_check_exception - Check for pending JNI exception
 *
 * If an exception is pending, logs it and throws a PostgreSQL ERROR.
 * call_context: Description of what JNI call was made (for error messages)
 */
static void
jni_check_exception(const char *call_context)
{
	jthrowable exc;

	if (Jenv == NULL)
		return;

	exc = (*Jenv)->ExceptionOccurred(Jenv);
	if (exc != NULL)
	{
		/* Log the Java exception details */
		elog(LOG, "JNI exception occurred in: %s", call_context);
		(*Jenv)->ExceptionDescribe(Jenv);
		(*Jenv)->ExceptionClear(Jenv);

		ereport(ERROR,
				(errmsg("JNI exception in %s", call_context),
				 errdetail("Check PostgreSQL logs for Java stack trace")));
	}
}

/* jq_cancel
 * 		Call cancel method from JDBCUtilsObject to release
 *		prepared statement and temporary result-set.
 */
void
jq_cancel(JDBCUtilsInfo * jdbcUtilsInfo)
{
	jclass		JDBCUtilsClass;
	jmethodID	id_cancel;
	jobject		utils_obj;

	/*
	 * CRITICAL: This function is called from transaction abort callback!
	 * We must NOT trigger errors that would create infinite recursion.
	 * Use WARNING for all failures and fail gracefully.
	 */

	if (jvm == NULL || Jenv == NULL || jdbcUtilsInfo == NULL)
		return;

	/* Read JDBCUtilsObject ONCE - multiple reads cause corruption */
	utils_obj = jdbcUtilsInfo->JDBCUtilsObject;
	if (utils_obj == NULL)
		return;

	/* Find the class - fail silently if not found */
	JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");
	if (JDBCUtilsClass == NULL)
	{
		elog(WARNING, "jdbc_fdw: Cannot find JDBCUtils class in jq_cancel");
		return;
	}

	/* Get the cancel method - fail silently if not found */
	id_cancel = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "cancel", "()V");
	if (id_cancel == NULL)
	{
		elog(WARNING, "jdbc_fdw: Cannot find cancel method in jq_cancel");
		(*Jenv)->DeleteLocalRef(Jenv, JDBCUtilsClass);
		return;
	}

	/* Call cancel and silently clear any exceptions */
	jq_exception_clear();
	(*Jenv)->CallObjectMethod(Jenv, utils_obj, id_cancel);

	/* Don't call jq_get_exception() - it would trigger ereport(ERROR)! */
	if ((*Jenv)->ExceptionCheck(Jenv))
	{
		elog(WARNING, "jdbc_fdw: Exception during cancel, clearing");
		(*Jenv)->ExceptionClear(Jenv);
	}

	(*Jenv)->DeleteLocalRef(Jenv, JDBCUtilsClass);
}


/*
 * jdbc_convert_string_to_cstring Uses a String object passed as a jobject to
 * the function to create an instance of C String.
 */
static char *
jdbc_convert_string_to_cstring(jobject java_cstring)
{
	jclass		JavaString;
	char	   *StringPointer;
	char	   *cString = NULL;

	JavaString = (*Jenv)->FindClass(Jenv, "java/lang/String");
	if (!((*Jenv)->IsInstanceOf(Jenv, java_cstring, JavaString)))
	{
		elog(ERROR, "Object not an instance of String class");
	}

	if (java_cstring != NULL)
	{
		StringPointer = (char *) (*Jenv)->GetStringUTFChars(Jenv,
															(jstring) java_cstring, 0);
		cString = pstrdup(StringPointer);
		(*Jenv)->ReleaseStringUTFChars(Jenv, (jstring) java_cstring, StringPointer);
		(*Jenv)->DeleteLocalRef(Jenv, java_cstring);
	}
	else
	{
		StringPointer = NULL;
	}
	return (cString);
}

/*
 * jdbc_convert_byte_array_to_datum Uses a byte array object passed as a jbyteArray to
 * the function to convert into Datum.
 */
static Datum
jdbc_convert_byte_array_to_datum(jbyteArray byteVal)
{
	Datum		valueDatum;
	jbyte	   *buf = (*Jenv)->GetByteArrayElements(Jenv, byteVal, NULL);
	jsize		size = (*Jenv)->GetArrayLength(Jenv, byteVal);

	if (buf == NULL)
		return 0;

	valueDatum = (Datum) palloc0(size + VARHDRSZ);
	memcpy(VARDATA(valueDatum), buf, size);
	SET_VARSIZE(valueDatum, size + VARHDRSZ);
	return valueDatum;
}

/*
 * jdbc_convert_object_to_datum Convert jobject to Datum value
 */
static Datum
jdbc_convert_object_to_datum(Oid pgtype, int32 pgtypmod, jobject obj)
{
	switch (pgtype)
	{
		case BYTEAOID:
			return jdbc_convert_byte_array_to_datum(obj);
		default:
			{
				/*
				 * By default, data is retrieved as string and then convert to
				 * compatible data types
				 */
				char	   *value = jdbc_convert_string_to_cstring(obj);

				if (value != NULL)
					return jdbc_convert_to_pg(pgtype, pgtypmod, value);
				else
					/* Return 0 if value is NULL */
					return 0;
			}
	}
}

/*
 * jdbc_destroy_jvm Shuts down the JVM.
 */
static void
jdbc_destroy_jvm()
{
	jint res;

	ereport(DEBUG3, (errmsg("In jdbc_destroy_jvm")));

	res = (*jvm)->DestroyJavaVM(jvm);

	/*
	 * jdbc_destroy_jvm will be called at on_proc_exit callback,
	 * so WARNING instead of ERROR for safe.
	 */
	if (res != JNI_OK)
		elog(WARNING, "jdbc_fdw: AttachCurrentThread failed with error code %d", res);
}

/*
 * ensure_jvm_ready - Ensure JVM is initialized and ready for use
 *
 * This function ensures the JVM is created and the JNI environment is set up.
 * It handles pgBouncer backend reuse detection and cleanup.
 */
static void
ensure_jvm_ready(const ForeignServer *server, const UserMapping *user)
{
	Oid current_userid = GetUserId();
	Oid current_dbid = MyDatabaseId;

	/* Check for pgBouncer backend reuse */
	if (jvm_initialized &&
	    (last_userid != current_userid || last_dbid != current_dbid))
	{
		elog(DEBUG1, "jdbc_fdw: Backend reuse detected (user %d->%d, db %d->%d)",
		     last_userid, current_userid, last_dbid, current_dbid);

		/* Reset connection state but keep JVM running */
		jdbc_reset_connection_cache();

		last_userid = current_userid;
		last_dbid = current_dbid;
	}

	/* Initialize JVM if needed */
	if (!jvm_initialized)
	{
		jdbc_jvm_init(server, user);
		jvm_initialized = true;
		last_userid = current_userid;
		last_dbid = current_dbid;
	}

	/* Verify JNI environment is valid */
	if (Jenv == NULL)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_ERROR),
		         errmsg("JNI environment not initialized after JVM creation")));
	}
}

/*
 * jdbc_detach_jvm Detach the JVM.
 * Currently unused - we keep JVM attached for backend lifetime to avoid
 * losing access to global JNI references like JDBCUtilsObject.
 */
#if 0
static void
jdbc_detach_jvm()
{
	ereport(DEBUG3, (errmsg("In jdbc_detach_jvm")));
	if (Jenv != NULL)
	{
		jint res = (*jvm)->DetachCurrentThread(jvm);

		/*
		 * jdbc_detach_jvm can be call at abort transaction callback,
		 * so WARNING instead of ERROR for safe
		 */
		if (res != JNI_OK)
			elog(WARNING, "jdbc_fdw: DetachCurrentThread failed with error code %d", res);
		Jenv = NULL;
	}
}
#endif

/*
 * jdbc_jvm_init Create the JVM which will be used for calling the Java
 * routines that use JDBC to connect and access the foreign database.
 *
 */
void
jdbc_jvm_init(const ForeignServer *server, const UserMapping *user)
{
	static bool FunctionCallCheck = false;	/* This flag safeguards against
											 * multiple calls of
											 * jdbc_jvm_init() */

	jint		res = -5;		/* Set to a negative value so we can see
								 * whether JVM has been correctly created or
								 * not */
	JavaVMInitArgs vm_args;
	char	   *classpath;
	char	   *maxheapsizeoption = NULL;

	opts.maxheapsize = 0;

	ereport(DEBUG3, (errmsg("In jdbc_jvm_init")));
	jdbc_get_server_options(&opts, server, user);	/* Get the maxheapsize
													 * value (if set) */

	if (FunctionCallCheck == false)
	{
		const char* env_classpath = getenv("CLASSPATH");
		int			option_index;

		vm_args.version = JNI_VERSION_1_2;
		vm_args.ignoreUnrecognized = JNI_FALSE;
		vm_args.nOptions = 3;		/* Base options: -Xrs, classpath, Arrow module access */

		if (env_classpath != NULL) {
			classpath = psprintf("-Djava.class.path=%s" PATH_SEPARATOR "%s", STR_SHAREEXTDIR, env_classpath);
		} else {
			classpath = psprintf("-Djava.class.path=%s", STR_SHAREEXTDIR);
		}


		if (opts.maxheapsize != 0)
		{
			/*
			 * If the user has given a value for setting the max heap size of
			 * the JVM
			 */
			maxheapsizeoption = psprintf("-Xmx%dm", opts.maxheapsize);
			vm_args.nOptions++;
		}
		vm_args.options = (JavaVMOption *) palloc0(sizeof(JavaVMOption) * vm_args.nOptions);

		option_index = 0;

		/*
		 * PostgreSQL must use its own signal handlers, so use -Xrs option to
		 * reduces the use of operating system signals by the JVM.
		 */
		vm_args.options[option_index++].optionString = "-Xrs";
		vm_args.options[option_index++].optionString = classpath;

		/*
		 * Required for Snowflake JDBC driver's Arrow memory allocation.
		 * See https://arrow.apache.org/docs/java/install.html
		 */
		vm_args.options[option_index++].optionString = "--add-opens=java.base/java.nio=ALL-UNNAMED";

		if (maxheapsizeoption != NULL)
		{
			vm_args.options[option_index++].optionString = maxheapsizeoption;
		}

		/* Create the Java VM - this also sets Jenv */
		res = JNI_CreateJavaVM(&jvm, (void **) &Jenv, &vm_args);
		if (res < 0)
		{
			ereport(ERROR, (errmsg("Failed to create Java VM, error code: %d", res)));
		}
		/* JNI_CreateJavaVM already attached the current thread, no need to call jdbc_attach_jvm */
		ereport(DEBUG3, (errmsg("Successfully created a JVM with %d MB heapsize and classpath set to '%s'", opts.maxheapsize, classpath)));
		/* Register an on_proc_exit handler that shuts down the JVM. */
		on_proc_exit(jdbc_destroy_jvm, 0);
		FunctionCallCheck = true;
	}
	else
	{
		int			JVMEnvStat;

		/* JVM already exists, verify environment */
		JVMEnvStat = (*jvm)->GetEnv(jvm, (void **) &Jenv, JNI_VERSION_1_2);
		if (JVMEnvStat == JNI_OK)
		{
			ereport(DEBUG3, (errmsg("JVM already initialized, Jenv=%p", (void *)Jenv)));
		}
		else
		{
			/* This shouldn't happen in our single-threaded model */
			ereport(ERROR, (errmsg("JVM exists but environment not available: %d", JVMEnvStat)));
		}
	}
}

/*
 * Create an actual JDBC connection to the foreign server. Precondition:
 * jdbc_jvm_init() has been successfully called. Returns: Jconn.status =
 * CONNECTION_OK and a valid reference to a JDBCUtils class Error return:
 * Jconn.status = CONNECTION_BAD
 */
static JDBCUtilsInfo *
jdbc_create_JDBC_connection(const ForeignServer *server, const UserMapping *user)
{
	jmethodID	idCreate;
	jmethodID	constructor;
	jstring		stringArray[6];
	jclass		javaString;
	jobjectArray argArray;
	jclass		JDBCUtilsClass;
	jmethodID	idGetIdentifierQuoteString;
	jstring		identifierQuoteString;
	char	   *quote_string;
	char	   *querytimeout_string;
	int			i;
	int			numParams = sizeof(stringArray) / sizeof(jstring);	/* Number of parameters
																	 * to Java */
	int			intSize = 10;	/* The string size to allocate for an integer
								 * value */
	int			keyid = user->umid; /* key for the hashtable in java depends
									 * on user mapping Oid  */
	JDBCUtilsInfo * volatile jdbcUtilsInfo;  /* volatile required for PG_TRY/PG_CATCH */
	jlong		server_hashvalue;
	jlong		mapping_hashvalue;
	JNIFrameGuard frame_guard;
	jobject		localRef = NULL;

	/*
	 * Allocate JDBCUtilsInfo in CacheMemoryContext.
	 * CRITICAL: Must use same memory context as the hash table to avoid
	 * corruption when accessing struct fields through hash table pointers.
	 * The corruption was NOT caused by palloc itself, but by reading the
	 * fields multiple times. We now read JDBCUtilsObject exactly once.
	 */
	MemoryContext oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	jdbcUtilsInfo = (JDBCUtilsInfo *) palloc0(sizeof(JDBCUtilsInfo));
	MemoryContextSwitchTo(oldcontext);

	ereport(DEBUG3, (errmsg("In jdbc_create_JDBC_connection")));
	jdbcUtilsInfo->status = CONNECTION_BAD;
	/* festate is query-specific and will be set when executing queries, not at connection time */
	jdbcUtilsInfo->festate = NULL;
	jdbcUtilsInfo->q_char = NULL;

	/* Ensure JVM and JNI environment are ready */
	ensure_jvm_ready(server, user);

	/* Clear any pending exceptions before starting */
	if ((*Jenv)->ExceptionCheck(Jenv))
	{
		(*Jenv)->ExceptionClear(Jenv);
	}

	/* Find the JDBCUtils class */
	JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");

	/* Check for Java exceptions */
	if ((*Jenv)->ExceptionCheck(Jenv))
	{
		elog(WARNING, "jdbc_fdw: Exception finding JDBCUtils class");
		(*Jenv)->ExceptionDescribe(Jenv);
		(*Jenv)->ExceptionClear(Jenv);
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_ERROR),
		         errmsg("Cannot find JDBCUtils class - check CLASSPATH")));
	}

	if (JDBCUtilsClass == NULL)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_ERROR),
		         errmsg("Failed to find the JDBCUtils class")));
	}
	idCreate = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "createConnection",
									"(IJJ[Ljava/lang/String;)V");
	if (idCreate == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.createConnection method!")));
	}
	idGetIdentifierQuoteString = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getIdentifierQuoteString", "()Ljava/lang/String;");
	if (idGetIdentifierQuoteString == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getIdentifierQuoteString method")));
	}

	/* Get the default constructor */
	constructor = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "<init>", "()V");
	if (constructor == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils default constructor")));
	}

	/*
	 * Construct the array to pass our parameters Query timeout is an int, we
	 * need a string
	 */
	querytimeout_string = (char *) palloc0(intSize);
	snprintf(querytimeout_string, intSize, "%d", opts.querytimeout);
	stringArray[0] = (*Jenv)->NewStringUTF(Jenv, opts.drivername);
	stringArray[1] = (*Jenv)->NewStringUTF(Jenv, opts.url);
	stringArray[2] = (*Jenv)->NewStringUTF(Jenv, opts.username);
	stringArray[3] = (*Jenv)->NewStringUTF(Jenv, opts.password);
	stringArray[4] = (*Jenv)->NewStringUTF(Jenv, querytimeout_string);
	stringArray[5] = (*Jenv)->NewStringUTF(Jenv, opts.jarfile);
	/* Set up the return value */
	javaString = (*Jenv)->FindClass(Jenv, "java/lang/String");
	argArray = (*Jenv)->NewObjectArray(Jenv, numParams, javaString, stringArray[0]);
	if (argArray == NULL)
	{
		/* Return Java memory */
		for (i = 0; i < numParams; i++)
		{
			(*Jenv)->DeleteLocalRef(Jenv, stringArray[i]);
		}
		ereport(ERROR, (errmsg("Failed to create argument array")));
	}
	for (i = 1; i < numParams; i++)
	{
		(*Jenv)->SetObjectArrayElement(Jenv, argArray, i, stringArray[i]);
	}

	/*
	 * Create JDBCUtils object with proper resource management.
	 * Use PG_TRY to ensure JNI frame is popped even on error.
	 */
	PG_TRY();
	{
		/* Push frame for all JNI operations in this block */
		frame_guard = jni_push_frame(32);

		elog(DEBUG1, "jdbc_fdw: Creating JDBCUtils instance");

		/* Create the JDBCUtils object */
		localRef = (*Jenv)->NewObject(Jenv, JDBCUtilsClass, constructor);
		jni_check_exception("NewObject(JDBCUtils)");

		if (localRef == NULL)
		{
			ereport(ERROR,
					(errmsg("Failed to create JDBCUtils object"),
					 errdetail("NewObject returned NULL")));
		}

		elog(DEBUG1, "jdbc_fdw: Creating global reference for JDBCUtils");

		/* Convert to global reference (this survives PopLocalFrame) */
		jdbcUtilsInfo->JDBCUtilsObject = (*Jenv)->NewGlobalRef(Jenv, localRef);
		jni_check_exception("NewGlobalRef(JDBCUtils)");

		/*
		 * CRITICAL: With palloc in CacheMemoryContext, we can now safely
		 * check the value. Read it ONCE into a local variable.
		 */
		jobject global_ref = jdbcUtilsInfo->JDBCUtilsObject;
		if (global_ref == NULL)
		{
			ereport(ERROR,
					(errmsg("Failed to create global reference for JDBCUtils object"),
					 errdetail("NewGlobalRef returned NULL")));
		}
		ereport(DEBUG3, (errmsg("Global reference created for JDBCUtils object")));

		/* Pop frame - this cleans up localRef but preserves the global ref */
		jni_pop_frame(&frame_guard, NULL);

		elog(DEBUG1, "jdbc_fdw: JDBCUtils object created successfully");
	}
	PG_CATCH();
	{
		/*
		 * CRITICAL: Do NOT access any struct fields in error handlers!
		 * Reading jdbcUtilsInfo->JDBCUtilsObject here can trigger infinite
		 * recursion, causing ERRORDATA_STACK_SIZE exceeded panic.
		 *
		 * Just pop the frame and re-throw. Cleanup will happen when the
		 * connection is released via normal cleanup paths.
		 */
		jni_pop_frame(&frame_guard, NULL);
		PG_RE_THROW();
	}
	PG_END_TRY();

	server_hashvalue = (jlong) GetSysCacheHashValue1(FOREIGNSERVEROID, ObjectIdGetDatum(server->serverid));
	mapping_hashvalue = (jlong) GetSysCacheHashValue1(USERMAPPINGOID, ObjectIdGetDatum(user->umid));

	/*
	 * CRITICAL: Read JDBCUtilsObject ONCE and use it.
	 * Do not read it multiple times - causes corruption!
	 */
	jobject utils_obj = jdbcUtilsInfo->JDBCUtilsObject;

	jq_exception_clear();
	(*Jenv)->CallObjectMethod(Jenv, utils_obj, idCreate, keyid, server_hashvalue, mapping_hashvalue, argArray);
	jq_get_exception();
	/* Return Java memory */
	for (i = 0; i < numParams; i++)
	{
		(*Jenv)->DeleteLocalRef(Jenv, stringArray[i]);
	}
	(*Jenv)->DeleteLocalRef(Jenv, argArray);
	ereport(DEBUG3, (errmsg("Created a JDBC connection: %s", opts.url)));

	/*
	 * CRITICAL: Reuse the utils_obj we read earlier at line 796.
	 * Do NOT read jdbcUtilsInfo->JDBCUtilsObject again!
	 */

	/* get default identifier quote string */
	jq_exception_clear();
	identifierQuoteString = (jstring) (*Jenv)->CallObjectMethod(Jenv, utils_obj, idGetIdentifierQuoteString);
	jq_get_exception();
	quote_string = jdbc_convert_string_to_cstring((jobject) identifierQuoteString);
	jdbcUtilsInfo->q_char = pstrdup(quote_string);
	jdbcUtilsInfo->status = CONNECTION_OK;
	pfree(querytimeout_string);

	/*
	 * CRITICAL: Do NOT access jdbcUtilsInfo->JDBCUtilsObject in ereport!
	 * Accessing struct fields during log message formatting causes the field
	 * to be zeroed out. Only access struct pointer addresses.
	 */
	ereport(DEBUG1,
			(errmsg("jdbc_fdw: created JDBC connection %p (not logging JDBCUtilsObject to avoid corruption)",
					jdbcUtilsInfo)));

	return jdbcUtilsInfo;
}

/*
 * Fetch the options for a jdbc_fdw foreign server and user mapping.
 */
static void
jdbc_get_server_options(JserverOptions * opts, const ForeignServer *f_server, const UserMapping *f_mapping)
{
	List	   *options;
	ListCell   *lc;

	/* Collect options from server and user mapping */
	options = NIL;
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_mapping->options);

	/* Loop through the options, and get the values */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "drivername") == 0)
		{
			opts->drivername = defGetString(def);
		}
		if (strcmp(def->defname, "username") == 0)
		{
			opts->username = defGetString(def);
		}
		if (strcmp(def->defname, "querytimeout") == 0)
		{
			opts->querytimeout = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "jarfile") == 0)
		{
			opts->jarfile = defGetString(def);
		}
		if (strcmp(def->defname, "maxheapsize") == 0)
		{
			opts->maxheapsize = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "password") == 0)
		{
			opts->password = defGetString(def);
		}
		if (strcmp(def->defname, "url") == 0)
		{
			opts->url = defGetString(def);
		}
	}
}

Jresult *
jq_exec(JDBCUtilsInfo * jdbcUtilsInfo, const char *query)
{
	jmethodID	idCreateStatement;
	jstring		statement;
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult    *res;

	ereport(DEBUG3, (errmsg("In jq_exec(%p): %s", jdbcUtilsInfo, query)));

	jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	idCreateStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "createStatement",
											 "(Ljava/lang/String;)V");
	if (idCreateStatement == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.createStatement method!")));
	}
	/* The query argument */
	statement = (*Jenv)->NewStringUTF(Jenv, query);
	if (statement == NULL)
	{
		ereport(ERROR, (errmsg("Failed to create query argument")));
	}
	jq_exception_clear();
	(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idCreateStatement, statement);
	jq_get_exception();
	/* Return Java memory */
	(*Jenv)->DeleteLocalRef(Jenv, statement);
	*res = PGRES_COMMAND_OK;
	return res;
}

Jresult *
jq_exec_id(JDBCUtilsInfo * jdbcUtilsInfo, const char *query, int *resultSetID)
{
	jmethodID	idCreateStatementID;
	jstring		statement;
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult    *res;

	ereport(DEBUG3, (errmsg("In jq_exec_id(%p): %s", jdbcUtilsInfo, query)));

	jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	idCreateStatementID = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "createStatementID",
											   "(Ljava/lang/String;)I");
	if (idCreateStatementID == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.createStatementID method!")));
	}
	/* The query argument */
	statement = (*Jenv)->NewStringUTF(Jenv, query);
	if (statement == NULL)
	{
		ereport(ERROR, (errmsg("Failed to create query argument")));
	}
	jq_exception_clear();
	*resultSetID = (int) (*Jenv)->CallIntMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idCreateStatementID, statement);
	jq_get_exception();
	if (*resultSetID < 0)
	{
		/* Return Java memory */
		(*Jenv)->DeleteLocalRef(Jenv, statement);
		ereport(ERROR, (errmsg("Get resultSetID failed with code: %d", *resultSetID)));
	}
	ereport(DEBUG3, (errmsg("Get resultSetID successfully, ID: %d", *resultSetID)));

	/* Return Java memory */
	(*Jenv)->DeleteLocalRef(Jenv, statement);
	*res = PGRES_COMMAND_OK;
	return res;
}

void *
jq_release_resultset_id(JDBCUtilsInfo * jdbcUtilsInfo, int resultSetID)
{
	jmethodID	idClearResultSetID;
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;

	ereport(DEBUG3, (errmsg("In jq_release_resultset_id: %d", resultSetID)));

	jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);

	idClearResultSetID = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "clearResultSetID",
											  "(I)V");
	if (idClearResultSetID == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.clearResultSetID method!")));
	}
	jq_exception_clear();
	(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idClearResultSetID, resultSetID);
	jq_get_exception();

	return NULL;
}

/*
 * jq_iterate: Read the next row from the remote server
 */
TupleTableSlot *
jq_iterate(JDBCUtilsInfo * jdbcUtilsInfo, ForeignScanState *node, List *retrieved_attrs, int resultSetID)
{
	jobject		JDBCUtilsObject;
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	TupleDesc	tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	jclass		JDBCUtilsClass;
	jmethodID	idResultSet;
	jmethodID	idNumberOfColumns;
	jobjectArray rowArray;
	int			numberOfColumns;
	int			i;

	ereport(DEBUG3, (errmsg("In jq_iterate")));

	memset(tupleSlot->tts_values, 0, sizeof(Datum) * tupleDescriptor->natts);
	memset(tupleSlot->tts_isnull, true, sizeof(bool) * tupleDescriptor->natts);

	jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);

	ExecClearTuple(tupleSlot);

	idNumberOfColumns = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getNumberOfColumns", "(I)I");
	if (idNumberOfColumns == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getNumberOfColumns method")));
	}
	jq_exception_clear();
	numberOfColumns = (int) (*Jenv)->CallIntMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idNumberOfColumns, resultSetID);
	jq_get_exception();
	if (numberOfColumns < 0)
	{
		ereport(ERROR, (errmsg("getNumberOfColumns got wrong value: %d", numberOfColumns)));
	}

	if ((*Jenv)->PushLocalFrame(Jenv, (numberOfColumns + 10)) < 0)
	{
		ereport(ERROR, (errmsg("Error pushing local java frame")));
	}

	idResultSet = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getResultSet", "(I)[Ljava/lang/Object;");
	if (idResultSet == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getResultSet method!")));
	}
	/* Allocate pointers to the row data */
	jq_exception_clear();
	rowArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, idResultSet, resultSetID);
	jq_get_exception();
	if (rowArray != NULL)
	{
		if (retrieved_attrs != NIL)
		{
			for (i = 0; i < retrieved_attrs->length; i++)
			{
				int			column_index = retrieved_attrs->elements[i].int_value - 1;
				Oid			pgtype = TupleDescAttr(tupleDescriptor, column_index)->atttypid;
				int32		pgtypmod = TupleDescAttr(tupleDescriptor, column_index)->atttypmod;
				jobject		obj = (jobject) (*Jenv)->GetObjectArrayElement(Jenv, rowArray, i);

				if (obj != NULL)
				{
					tupleSlot->tts_isnull[column_index] = false;
					tupleSlot->tts_values[column_index] = jdbc_convert_object_to_datum(pgtype, pgtypmod, obj);
				}
			}
		}
		ExecStoreVirtualTuple(tupleSlot);
		(*Jenv)->DeleteLocalRef(Jenv, rowArray);
	}
	(*Jenv)->PopLocalFrame(Jenv, NULL);
	return (tupleSlot);
}

/*
 * jq_iterate_all_row: Read the all row from the remote server without an existing foreign table
 */
void
jq_iterate_all_row(FunctionCallInfo fcinfo, JDBCUtilsInfo * jdbcUtilsInfo, TupleDesc tupleDescriptor, int resultSetID)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	jobject		JDBCUtilsObject;
	jclass		JDBCUtilsClass;

	jmethodID	idResultSet;
	jmethodID	idNumberOfColumns;
	jobjectArray rowArray;

	Tuplestorestate *tupstore;
	HeapTuple	tuple = NULL;

	MemoryContext oldcontext;

	Datum	   *values;
	bool	   *nulls;

	int			numberOfColumns;

	ereport(DEBUG3, (errmsg("In jq_iterate_all_row")));

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);

	jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);

	idNumberOfColumns = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getNumberOfColumns", "(I)I");
	if (idNumberOfColumns == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getNumberOfColumns method")));
	}
	jq_exception_clear();
	numberOfColumns = (int) (*Jenv)->CallIntMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idNumberOfColumns, resultSetID);
	jq_get_exception();
	if (numberOfColumns < 0)
	{
		ereport(ERROR, (errmsg("getNumberOfColumns got wrong value: %d", numberOfColumns)));
	}

	if ((*Jenv)->PushLocalFrame(Jenv, (numberOfColumns + 10)) < 0)
	{
		ereport(ERROR, (errmsg("Error pushing local java frame")));
	}

	idResultSet = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getResultSet", "(I)[Ljava/lang/Object;");
	if (idResultSet == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getResultSet method!")));
	}

	do
	{
		/* Allocate pointers to the row data */
		jq_exception_clear();
		rowArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, idResultSet, resultSetID);
		jq_get_exception();

		if (rowArray != NULL)
		{
			values = (Datum *) palloc0(tupleDescriptor->natts * sizeof(Datum));
			nulls = (bool *) palloc(tupleDescriptor->natts * sizeof(bool));
			/* Initialize to nulls for any columns not present in result */
			memset(nulls, true, tupleDescriptor->natts * sizeof(bool));

			for (int i = 0; i < numberOfColumns; i++)
			{
				int			column_index = i;
				Oid			pgtype = TupleDescAttr(tupleDescriptor, column_index)->atttypid;
				int32		pgtypmod = TupleDescAttr(tupleDescriptor, column_index)->atttypmod;
				jobject		obj = (jobject) (*Jenv)->GetObjectArrayElement(Jenv, rowArray, i);

				if (obj != NULL)
				{
					values[column_index] = jdbc_convert_object_to_datum(pgtype, pgtypmod, obj);
					nulls[column_index] = false;
				}
			}

			tuple = heap_form_tuple(tupleDescriptor, values, nulls);
			tuplestore_puttuple(tupstore, tuple);

			(*Jenv)->DeleteLocalRef(Jenv, rowArray);
		}
	}
	while (rowArray != NULL);

	if (tuple != NULL)
	{
		rsinfo->setResult = tupstore;
		rsinfo->setDesc = tupleDescriptor;
		MemoryContextSwitchTo(oldcontext);
	}

	(*Jenv)->PopLocalFrame(Jenv, NULL);
}



Jresult *
jq_exec_prepared(JDBCUtilsInfo * jdbcUtilsInfo, const int *paramLengths,
				 const int *paramFormats, int resultFormat, int resultSetID)
{
	jmethodID	idExecPreparedStatement;
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult    *res;

	ereport(DEBUG3, (errmsg("In jq_exec_prepared")));

	jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	idExecPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "execPreparedStatement",
												   "(I)V");
	if (idExecPreparedStatement == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.execPreparedStatement method!")));
	}
	jq_exception_clear();
	(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idExecPreparedStatement, resultSetID);
	jq_get_exception();

	/* Return Java memory */
	*res = PGRES_COMMAND_OK;

	return res;
}

void
jq_clear(Jresult * res)
{
	ereport(DEBUG3, (errmsg("In jq_clear")));
	pfree(res);
	return;
}

char *
jq_cmd_tuples(Jresult * res)
{
	ereport(DEBUG3, (errmsg("In jq_cmd_tuples")));
	return 0;
}

char *
jq_get_value(const Jresult * res, int tup_num, int field_num)
{
	ereport(DEBUG3, (errmsg("In jq_get_value")));
	return 0;
}

Jresult *
jq_prepare(JDBCUtilsInfo * jdbcUtilsInfo, const char *query,
		   const Oid *paramTypes, int *resultSetID)
{
	jmethodID	idCreatePreparedStatement;
	jstring		statement;
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult    *res;

	ereport(DEBUG3, (errmsg("In jq_prepare(%p): %s", jdbcUtilsInfo, query)));

	jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	idCreatePreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "createPreparedStatement",
													 "(Ljava/lang/String;)I");
	if (idCreatePreparedStatement == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.createPreparedStatement method!")));
	}
	/* The query argument */
	statement = (*Jenv)->NewStringUTF(Jenv, query);
	if (statement == NULL)
	{
		ereport(ERROR, (errmsg("Failed to create query argument")));
	}
	jq_exception_clear();
	/* get the resultSetID */
	*resultSetID = (int) (*Jenv)->CallIntMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idCreatePreparedStatement, statement);
	jq_get_exception();
	if (*resultSetID < 0)
	{
		/* Return Java memory */
		(*Jenv)->DeleteLocalRef(Jenv, statement);
		ereport(ERROR, (errmsg("Get resultSetID failed with code: %d", *resultSetID)));
	}
	ereport(DEBUG3, (errmsg("Get resultSetID successfully, ID: %d", *resultSetID)));

	/* Return Java memory */
	(*Jenv)->DeleteLocalRef(Jenv, statement);
	*res = PGRES_COMMAND_OK;

	return res;
}

int
jq_nfields(const Jresult * res)
{
	ereport(DEBUG3, (errmsg("In jq_nfields")));
	return 0;
}

int
jq_get_is_null(const Jresult * res, int tup_num, int field_num)
{
	ereport(DEBUG3, (errmsg("In jq_get_is_null")));
	return 0;
}

JDBCUtilsInfo *
jq_connect_db_params(const ForeignServer *server, const UserMapping *user,
					 const char *const *keywords, const char *const *values)
{
	JDBCUtilsInfo *jdbcUtilsInfo;
	int			i = 0;

	ereport(DEBUG3, (errmsg("In jq_connect_db_params")));
	while (keywords[i])
	{
		const char *pvalue = values[i];

		if (pvalue == NULL && pvalue[0] == '\0')
		{
			break;
		}
		i++;
	}
	/* Initialize the Java JVM (if it has not been done already) */
	jdbc_jvm_init(server, user);
	jdbcUtilsInfo = jdbc_create_JDBC_connection(server, user);
	if (jq_status(jdbcUtilsInfo) == CONNECTION_BAD)
	{
		(void) jdbc_connect_db_complete(jdbcUtilsInfo);
	}
	return jdbcUtilsInfo;
}

/*
 * Do any cleanup needed and close a database connection Return 1 on success,
 * 0 on failure
 */
static int
jdbc_connect_db_complete(JDBCUtilsInfo * jdbcUtilsInfo)
{
	ereport(DEBUG3, (errmsg("In jdbc_connect_db_complete")));
	return 0;
}

ConnStatusType
jq_status(const JDBCUtilsInfo * jdbcUtilsInfo)
{
	if (!jdbcUtilsInfo)
	{
		return CONNECTION_BAD;
	}
	return jdbcUtilsInfo->status;
}

char *
jq_error_message(const JDBCUtilsInfo * jdbcUtilsInfo)
{
	ereport(DEBUG3, (errmsg("In jq_error_message")));
	return "Unknown Error!";
}

void
jq_finish(void)
{
	/* Clear any pending Java exceptions but do NOT detach from JVM.
	 * The JVM should stay alive for the lifetime of the PostgreSQL backend.
	 */
	if (Jenv != NULL && (*Jenv)->ExceptionCheck(Jenv))
	{
		(*Jenv)->ExceptionClear(Jenv);
	}
	return;
}

int
jq_server_version(const JDBCUtilsInfo * jdbcUtilsInfo)
{
	ereport(DEBUG3, (errmsg("In jq_server_version")));
	return 0;
}

char *
jq_result_error_field(const Jresult * res, int fieldcode)
{
	ereport(DEBUG3, (errmsg("In jq_result_error_field")));
	return 0;
}

PGTransactionStatusType
jq_transaction_status(const JDBCUtilsInfo * jdbcUtilsInfo)
{
	ereport(DEBUG3, (errmsg("In jq_transaction_status")));
	return PQTRANS_UNKNOWN;
}

void *
jq_bind_sql_var(JDBCUtilsInfo * jdbcUtilsInfo, Oid type, int attnum, Datum value, bool *isnull, int resultSetID)
{
	jmethodID	idBindPreparedStatement;
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult    *res;
	int			nestlevel;

	ereport(DEBUG3, (errmsg("In jq_bind_sql_var")));

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);

	attnum++;
	elog(DEBUG2, "jdbc_fdw : %s %d type=%u ", __func__, attnum, type);

	if (*isnull)
	{
		idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindNullPreparedStatement",
													   "(II)V");
		if (idBindPreparedStatement == NULL)
		{
			ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bind method!")));
		}
		jq_exception_clear();
		(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, attnum, resultSetID);
		jq_get_exception();
		*res = PGRES_COMMAND_OK;
		return NULL;
	}

	switch (type)
	{
		case INT2OID:
			{
				int16		dat = DatumGetInt16(value);

				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindIntPreparedStatement",
															   "(III)V");
				if (idBindPreparedStatement == NULL)
				{
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindInt method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case INT4OID:
			{
				int32		dat = DatumGetInt32(value);

				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindIntPreparedStatement",
															   "(III)V");
				if (idBindPreparedStatement == NULL)
				{
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindInt method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case INT8OID:
			{
				int64		dat = DatumGetInt64(value);

				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindLongPreparedStatement",
															   "(JII)V");
				if (idBindPreparedStatement == NULL)
				{
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindLong method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}

		case FLOAT4OID:

			{
				float4		dat = DatumGetFloat4(value);

				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindFloatPreparedStatement",
															   "(FII)V");
				if (idBindPreparedStatement == NULL)
				{
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindFloat method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case FLOAT8OID:
			{
				float8		dat = DatumGetFloat8(value);

				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindDoublePreparedStatement",
															   "(DII)V");
				if (idBindPreparedStatement == NULL)
				{
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindDouble method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}

		case NUMERICOID:
			{
				Datum		valueDatum = DirectFunctionCall1(numeric_float8, value);
				float8		dat = DatumGetFloat8(valueDatum);

				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindDoublePreparedStatement",
															   "(DII)V");
				if (idBindPreparedStatement == NULL)
				{
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindDouble method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case BOOLOID:
			{
				bool		dat = (bool) value;

				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindBooleanPreparedStatement",
															   "(ZII)V");
				if (idBindPreparedStatement == NULL)
				{
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindBoolean method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}

		case BYTEAOID:
			{
				long		len;
				char	   *dat = NULL;
				char	   *result = DatumGetPointer(value);
				jbyteArray	retArray;

				if (VARATT_IS_1B(result))
				{
					len = VARSIZE_1B(result) - VARHDRSZ_SHORT;
					dat = VARDATA_1B(result);
				}
				else
				{
					len = VARSIZE_4B(result) - VARHDRSZ;
					dat = VARDATA_4B(result);
				}

				retArray = (*Jenv)->NewByteArray(Jenv, len);
				(*Jenv)->SetByteArrayRegion(Jenv, retArray, 0, len, (jbyte *) (dat));


				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindByteaPreparedStatement",
															   "([BJII)V");
				if (idBindPreparedStatement == NULL)
				{
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindBytea method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, retArray, len, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		case NAMEOID:
			{
				/* Bind as text */
				char	   *outputString = NULL;
				jstring		dat = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				dat = (*Jenv)->NewStringUTF(Jenv, outputString);
				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindStringPreparedStatement",
															   "(Ljava/lang/String;II)V");
				if (idBindPreparedStatement == NULL)
				{
					/* Return Java memory */
					(*Jenv)->DeleteLocalRef(Jenv, dat);
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindString method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();

				/* Return Java memory */
				(*Jenv)->DeleteLocalRef(Jenv, dat);
				break;
			}
		case TIMEOID:
			{
				/* Bind as text */
				char	   *outputString = NULL;
				jstring		dat = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				dat = (*Jenv)->NewStringUTF(Jenv, outputString);
				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindTimePreparedStatement",
															   "(Ljava/lang/String;II)V");
				if (idBindPreparedStatement == NULL)
				{
					/* Return Java memory */
					(*Jenv)->DeleteLocalRef(Jenv, dat);
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindTime method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();

				/* Return Java memory */
				(*Jenv)->DeleteLocalRef(Jenv, dat);
				break;
			}
		case TIMETZOID:
			{
				/* Bind as text */
				char	   *outputString = NULL;
				jstring		dat = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				dat = (*Jenv)->NewStringUTF(Jenv, outputString);
				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindTimeTZPreparedStatement",
															   "(Ljava/lang/String;II)V");
				if (idBindPreparedStatement == NULL)
				{
					/* Return Java memory */
					(*Jenv)->DeleteLocalRef(Jenv, dat);
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindTimeTZ method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();

				/* Return Java memory */
				(*Jenv)->DeleteLocalRef(Jenv, dat);
				break;
			}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				/*
				 * Bind as microseconds from Unix Epoch time in UTC time zone
				 * to avoid being affected by JVM's time zone.
				 */
				Timestamp	valueTimestamp = DatumGetTimestamp(value);	/* Already in UTC time
																		 * zone */
				int64		valueMicroSecs = valueTimestamp + POSTGRES_TO_UNIX_EPOCH_USECS;

				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindTimestampPreparedStatement",
															   "(JII)V");
				if (idBindPreparedStatement == NULL)
				{
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindTimestamp method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, valueMicroSecs, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case DATEOID:
			{
				/* Bind as text */
				char	   *outputString = NULL;
				jstring		dat = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				/*
				 * Make sure the DATE value is unambiguous to the remote
				 * server
				 */
				nestlevel = jdbc_set_transmission_modes();

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);

				jdbc_reset_transmission_modes(nestlevel);

				dat = (*Jenv)->NewStringUTF(Jenv, outputString);
				idBindPreparedStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "bindDatePreparedStatement",
															   "(Ljava/lang/String;II)V");
				if (idBindPreparedStatement == NULL)
				{
					/* Return Java memory */
					(*Jenv)->DeleteLocalRef(Jenv, dat);
					ereport(ERROR, (errmsg("Failed to find the JDBCUtils.bindDatePreparedStatement method!")));
				}
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, jdbcUtilsInfo->JDBCUtilsObject, idBindPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();

				/* Return Java memory */
				(*Jenv)->DeleteLocalRef(Jenv, dat);

				break;
			}
		default:
			{
				ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("cannot convert constant value to JDBC value %u", type),
								errhint("Constant value data type: %u", type)));
				break;
			}
	}
	*res = PGRES_COMMAND_OK;
	return 0;
}

/*
 * jdbc_convert_to_pg: Convert JDBC data into PostgreSQL's compatible data
 * types
 */
Datum
jdbc_convert_to_pg(Oid pgtyp, int pgtypmod, char *value)
{
	Datum		valueDatum;
	Datum		stringDatum;
	regproc		typeinput;
	HeapTuple	tuple;
	int			typemod;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtyp));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type%u", pgtyp);

	typeinput = ((Form_pg_type) GETSTRUCT(tuple))->typinput;
	typemod = ((Form_pg_type) GETSTRUCT(tuple))->typtypmod;
	ReleaseSysCache(tuple);

	stringDatum = CStringGetDatum(value);
	valueDatum = OidFunctionCall3(typeinput, stringDatum,
								  ObjectIdGetDatum(pgtyp),
								  Int32GetDatum(typemod));

	return valueDatum;
}

/*
 * jq_exception_clear: clears any exception that is currently being thrown
 */
void
jq_exception_clear()
{
	(*Jenv)->ExceptionClear(Jenv);
	return;
}

/*
 * jq_get_exception: get the JNI exception is currently being thrown convert
 * to String for outputting error message
 *
 * CRITICAL: This function must NOT call ereport(ERROR) if it can't get the
 * exception message, as that would cause infinite recursion during error handling.
 */
void
jq_get_exception()
{
	/* check for pending exceptions */
	if ((*Jenv)->ExceptionCheck(Jenv))
	{
		jthrowable	exc;
		jmethodID	exceptionMsgID;
		jclass		objectClass;
		jstring		exceptionMsg;
		char	   *exceptionString;
		char	   *err_msg = NULL;

		/* determines if an exception is being thrown */
		exc = (*Jenv)->ExceptionOccurred(Jenv);

		/* Print exception details to stderr for debugging */
		elog(LOG, "Java exception occurred, printing stack trace:");
		(*Jenv)->ExceptionDescribe(Jenv);

		/* get to the message and stack trace one as String */
		objectClass = (*Jenv)->FindClass(Jenv, "java/lang/Object");
		if (objectClass == NULL)
		{
			/*
			 * CRITICAL: Don't call ereport(ERROR) here! It would cause infinite
			 * recursion if we're already in error handling. Just clear and report generic error.
			 */
			(*Jenv)->ExceptionClear(Jenv);
			ereport(ERROR, (errmsg("remote server returned a Java exception (details unavailable - could not load java/lang/Object class)")));
			return;  /* unreachable, but for clarity */
		}

		exceptionMsgID = (*Jenv)->GetMethodID(Jenv, objectClass, "toString", "()Ljava/lang/String;");
		if (exceptionMsgID == NULL)
		{
			/* Fail gracefully if we can't get toString method */
			(*Jenv)->DeleteLocalRef(Jenv, objectClass);
			(*Jenv)->ExceptionClear(Jenv);
			ereport(ERROR, (errmsg("remote server returned a Java exception (details unavailable - could not find toString method)")));
			return;
		}

		/* Get exception message - this itself could throw, so be careful */
		exceptionMsg = (jstring) (*Jenv)->CallObjectMethod(Jenv, exc, exceptionMsgID);
		(*Jenv)->DeleteLocalRef(Jenv, objectClass);

		/* Check if getting the message threw another exception */
		if ((*Jenv)->ExceptionCheck(Jenv))
		{
			(*Jenv)->ExceptionClear(Jenv);
			ereport(ERROR, (errmsg("remote server returned a Java exception (details unavailable - error getting exception message)")));
			return;
		}

		if (exceptionMsg == NULL)
		{
			ereport(ERROR, (errmsg("remote server returned a Java exception (message was null)")));
			return;
		}

		exceptionString = jdbc_convert_string_to_cstring((jobject) exceptionMsg);
		err_msg = pstrdup(exceptionString);
		ereport(ERROR, (errmsg("remote server returned an error: %s", err_msg)));
	}
	return;
}

static List *
jq_get_column_infos(JDBCUtilsInfo * jdbcUtilsInfo, char *tablename)
{
	jobject		JDBCUtilsObject;
	jclass		JDBCUtilsClass;
	jstring		jtablename = NULL;
	jobjectArray columnNameArray;
	jobjectArray columnTypeArray;
	jobjectArray primaryKeyArray;  /* Array of primary key column names */
	jsize		numberOfColumns;
	jsize		numberOfPrimaryKeys = 0;
	List	   *columnlist = NIL;
	int			i;
	JNIFrameGuard frame_guard;

	/* Get JDBCUtils */
	PG_TRY();
	{
		jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);
	}
	PG_CATCH();
	{
		/* Just rethrow - don't hide the original error message */
		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_TRY();
	{
		jmethodID idGetColumnNames;
		jmethodID idGetColumnTypes;
		jmethodID idGetPrimaryKeys;

		/* Push frame for all JNI operations */
		frame_guard = jni_push_frame(64);

		/* Convert table name to Java string */
		jtablename = (*Jenv)->NewStringUTF(Jenv, tablename);
		jni_check_exception("NewStringUTF(tablename)");

		/* Get method IDs */
		idGetColumnNames = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getColumnNames",
												"(Ljava/lang/String;)[Ljava/lang/String;");
		if (idGetColumnNames == NULL)
			ereport(ERROR, (errmsg("Failed to find JDBCUtils.getColumnNames method")));

		idGetColumnTypes = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getColumnTypes",
												"(Ljava/lang/String;)[Ljava/lang/String;");
		if (idGetColumnTypes == NULL)
			ereport(ERROR, (errmsg("Failed to find JDBCUtils.getColumnTypes method")));

		idGetPrimaryKeys = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getPrimaryKey",
												"(Ljava/lang/String;)[Ljava/lang/String;");
		if (idGetPrimaryKeys == NULL)
			ereport(ERROR, (errmsg("Failed to find JDBCUtils.getPrimaryKey method")));

		/* Call Java methods */
		jq_exception_clear();
		columnNameArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject,
													idGetColumnNames, jtablename);
		jni_check_exception("CallObjectMethod(getColumnNames)");

		columnTypeArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject,
													idGetColumnTypes, jtablename);
		jni_check_exception("CallObjectMethod(getColumnTypes)");

		primaryKeyArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject,
													idGetPrimaryKeys, jtablename);
		jni_check_exception("CallObjectMethod(getPrimaryKey)");

		/* Get number of primary key columns */
		if (primaryKeyArray != NULL)
			numberOfPrimaryKeys = (*Jenv)->GetArrayLength(Jenv, primaryKeyArray);

		if (columnNameArray != NULL)
		{
			numberOfColumns = (*Jenv)->GetArrayLength(Jenv, columnNameArray);
			elog(DEBUG1, "jdbc_fdw: Table '%s' has %d columns, %d primary keys",
				 tablename, (int)numberOfColumns, (int)numberOfPrimaryKeys);

			for (i = 0; i < numberOfColumns; i++)
			{
				JcolumnInfo *columnInfo = (JcolumnInfo *) palloc0(sizeof(JcolumnInfo));
				jobject nameObj;
				jobject typeObj;
				bool isPrimaryKey = false;
				int j;

				/* Get column name */
				nameObj = (*Jenv)->GetObjectArrayElement(Jenv, columnNameArray, i);
				columnInfo->column_name = jdbc_convert_string_to_cstring(nameObj);

				/* Get column type */
				typeObj = (*Jenv)->GetObjectArrayElement(Jenv, columnTypeArray, i);
				columnInfo->column_type = jdbc_convert_string_to_cstring(typeObj);

				/* Check if this column is in the primary key array */
				if (primaryKeyArray != NULL)
				{
					for (j = 0; j < numberOfPrimaryKeys; j++)
					{
						jobject pkNameObj = (*Jenv)->GetObjectArrayElement(Jenv, primaryKeyArray, j);
						if (pkNameObj != NULL)
						{
							char *pkName = jdbc_convert_string_to_cstring(pkNameObj);
							if (strcmp(columnInfo->column_name, pkName) == 0)
							{
								isPrimaryKey = true;
								break;
							}
						}
					}
				}
				columnInfo->primary_key = isPrimaryKey;

				elog(DEBUG1, "jdbc_fdw: Column %d: name=%s, type=%s, pk=%d",
					 i, columnInfo->column_name, columnInfo->column_type, columnInfo->primary_key);

				columnlist = lappend(columnlist, columnInfo);
			}
		}
		else
		{
			elog(WARNING, "jdbc_fdw: getColumnNames returned NULL for table '%s'", tablename);
		}

		/* Pop frame - cleanup all JNI references */
		jni_pop_frame(&frame_guard, NULL);
	}
	PG_CATCH();
	{
		/* Ensure frame cleanup */
		jni_pop_frame(&frame_guard, NULL);
		/* Just rethrow - don't hide the original error message */
		PG_RE_THROW();
	}
	PG_END_TRY();

	return columnlist;
}

/*
 * jq_get_column_infos_without_key
 *
 * This function is clone from jq_get_column_infos
 * to get column names and data types.
 * Primary key is set to false because it is not necessary.
 *
 */
List *
jq_get_column_infos_without_key(JDBCUtilsInfo * jdbcUtilsInfo, int *resultSetID, int *column_num)
{
	jobject		JDBCUtilsObject;
	jclass		JDBCUtilsClass;
	int			i;

	/* getColumnNames */
	jmethodID	idGetColumnNamesByResultSetID;
	jobjectArray columnNamesArray;
	jsize		numberOfNames;

	/* getColumnTypes */
	jmethodID	idGetColumnTypesByResultSetID;
	jobjectArray columnTypesArray;
	jsize		numberOfTypes;

	/* getColumnNumber */
	jmethodID	idNumberOfColumns;
	jint		jresultSetID = *resultSetID;
	int			numberOfColumns;

	/* for generating columnInfo List */
	List	   *columnInfoList = NIL;
	JcolumnInfo *columnInfo;

	/* Get JDBCUtils */
	PG_TRY();
	{
		jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* getColumnNames by resultSetID */
	idGetColumnNamesByResultSetID = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getColumnNamesByResultSetID", "(I)[Ljava/lang/String;");
	if (idGetColumnNamesByResultSetID == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getColumnNamesByResultSetID method")));
	}
	jq_exception_clear();
	columnNamesArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, idGetColumnNamesByResultSetID, jresultSetID);
	jq_get_exception();

	/* getColumnTypes by resultSetID */
	idGetColumnTypesByResultSetID = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getColumnTypesByResultSetID", "(I)[Ljava/lang/String;");
	if (idGetColumnTypesByResultSetID == NULL)
	{
		if (columnNamesArray != NULL)
		{
			(*Jenv)->DeleteLocalRef(Jenv, columnNamesArray);
		}
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getColumnTypesByResultSetID method")));
	}
	jq_exception_clear();
	columnTypesArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, idGetColumnTypesByResultSetID, jresultSetID);
	jq_get_exception();

	/* getNumberOfColumns */
	idNumberOfColumns = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getNumberOfColumns", "(I)I");
	if (idNumberOfColumns == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getNumberOfColumns method")));
	}
	jq_exception_clear();
	numberOfColumns = (int) (*Jenv)->CallIntMethod(Jenv, JDBCUtilsObject, idNumberOfColumns, jresultSetID);
	*column_num = numberOfColumns;
	jq_get_exception();

	if (columnNamesArray != NULL && columnTypesArray != NULL)
	{
		numberOfNames = (*Jenv)->GetArrayLength(Jenv, columnNamesArray);
		numberOfTypes = (*Jenv)->GetArrayLength(Jenv, columnTypesArray);

		if (numberOfNames != numberOfTypes)
		{
			(*Jenv)->DeleteLocalRef(Jenv, columnTypesArray);
			(*Jenv)->DeleteLocalRef(Jenv, columnNamesArray);
			ereport(ERROR, (errmsg("Cannot get the dependable columnInfo.")));
		}

		for (i = 0; i < numberOfNames; i++)
		{
			/* init columnInfo */
			char	   *tmpColumnNames = jdbc_convert_string_to_cstring((jobject) (*Jenv)->GetObjectArrayElement(Jenv, columnNamesArray, i));
			char	   *tmpColumnTypes = jdbc_convert_string_to_cstring((jobject) (*Jenv)->GetObjectArrayElement(Jenv, columnTypesArray, i));

			columnInfo = (JcolumnInfo *) palloc0(sizeof(JcolumnInfo));
			columnInfo->column_name = tmpColumnNames;
			columnInfo->column_type = tmpColumnTypes;
			columnInfo->primary_key = false;

			columnInfoList = lappend(columnInfoList, columnInfo);
		}
	}

	if (columnNamesArray != NULL)
	{
		(*Jenv)->DeleteLocalRef(Jenv, columnNamesArray);
	}
	if (columnTypesArray != NULL)
	{
		(*Jenv)->DeleteLocalRef(Jenv, columnTypesArray);
	}

	return columnInfoList;
}

/*
 * jq_get_table_names
 *      Retrieves table names from the foreign database
 *      Parameters:
 *        jdbcUtilsInfo - JDBC connection info
 *        schemaPattern - schema name to filter (NULL means all schemas)
 *        tableNames - comma-separated list of table names (NULL means all tables)
 */
static List *
jq_get_table_names(JDBCUtilsInfo * jdbcUtilsInfo, const char *schemaPattern, const char *tableNames)
{
	jobject		JDBCUtilsObject;
	jclass		JDBCUtilsClass;
	jmethodID	idGetTableNames;
	jobjectArray tableNameArray;
	List	   *tableName = NIL;
	jsize		numberOfTables;
	int			i;
	jstring		jSchemaPattern = NULL;
	jstring		jTableNames = NULL;
	JNIFrameGuard frame_guard;

	/*
	 * CRITICAL: Do not read jdbcUtilsInfo->JDBCUtilsObject here!
	 * jq_get_JDBCUtils will read it exactly once.
	 */

	jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);

	/* Use the new method signature that accepts schema and table filters */
	idGetTableNames = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "getTableNames",
										   "(Ljava/lang/String;Ljava/lang/String;)[Ljava/lang/String;");
	if (idGetTableNames == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getTableNames(String, String) method")));
	}

	PG_TRY();
	{
		/* Push frame for all JNI string operations */
		frame_guard = jni_push_frame(16);

		/* Convert C strings to Java strings (NULL stays as NULL) */
		if (schemaPattern != NULL)
		{
			jSchemaPattern = (*Jenv)->NewStringUTF(Jenv, schemaPattern);
			jni_check_exception("NewStringUTF(schemaPattern)");
		}
		if (tableNames != NULL)
		{
			jTableNames = (*Jenv)->NewStringUTF(Jenv, tableNames);
			jni_check_exception("NewStringUTF(tableNames)");
		}

		jq_exception_clear();
		tableNameArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, idGetTableNames,
												   jSchemaPattern, jTableNames);
		jni_check_exception("CallObjectMethod(getTableNames)");

		if (tableNameArray != NULL)
		{
			numberOfTables = (*Jenv)->GetArrayLength(Jenv, tableNameArray);
			elog(DEBUG1, "jdbc_fdw: Retrieved %d tables from schema '%s'",
				 (int)numberOfTables,
				 schemaPattern ? schemaPattern : "(all schemas)");

			for (i = 0; i < numberOfTables; i++)
			{
				jobject strObj = (*Jenv)->GetObjectArrayElement(Jenv, tableNameArray, i);
				char *tmpTableName = jdbc_convert_string_to_cstring(strObj);

				tableName = lappend(tableName, tmpTableName);
			}
		}

		/* Pop frame - all local JNI references cleaned up */
		jni_pop_frame(&frame_guard, NULL);
	}
	PG_CATCH();
	{
		/* Ensure frame cleanup on error */
		jni_pop_frame(&frame_guard, NULL);
		PG_RE_THROW();
	}
	PG_END_TRY();

	elog(DEBUG1, "jdbc_fdw: jq_get_table_names returning list with %d tables",
		 list_length(tableName));
	return tableName;
}

/*
 * jq_get_schema_info
 *      Retrieves schema information (tables and columns) from the foreign database
 *      Parameters:
 *        jdbcUtilsInfo - JDBC connection info
 *        schemaPattern - schema name to filter (NULL means all schemas)
 *        tableNames - comma-separated list of table names (NULL means all tables)
 */
List *
jq_get_schema_info(JDBCUtilsInfo * jdbcUtilsInfo, const char *schemaPattern, const char *tableNames)
{
	List	   *schema_list = NIL;
	List	   *tableName = NIL;
	JtableInfo *tableInfo;
	ListCell   *lc;

	/* Validate input parameters */
	if (jdbcUtilsInfo == NULL)
	{
		ereport(ERROR, (errmsg("jq_get_schema_info: jdbcUtilsInfo is NULL")));
	}

	/*
	 * CRITICAL: Do NOT read jdbcUtilsInfo->JDBCUtilsObject here!
	 * Reading fields from malloc'd structs causes corruption.
	 * jq_get_JDBCUtils will read it exactly once.
	 */

	tableName = jq_get_table_names(jdbcUtilsInfo, schemaPattern, tableNames);

	if (tableName == NIL)
		return NIL;

	foreach(lc, tableName)
	{
		char	   *tmpTableName = NULL;

		tmpTableName = (char *) lfirst(lc);

		tableInfo = (JtableInfo *) palloc0(sizeof(JtableInfo));
		if (tmpTableName != NULL)
		{
			tableInfo->table_name = tmpTableName;
			tableInfo->column_info = jq_get_column_infos(jdbcUtilsInfo, tmpTableName);
			schema_list = lappend(schema_list, tableInfo);
		}
	}

	return schema_list;
}

/*
 * jq_get_type_warnings
 *      Returns type conversion warnings for a given table
 */
char **
jq_get_type_warnings(JDBCUtilsInfo * jdbcUtilsInfo, char *tableName)
{
	jobject		JDBCUtilsObject;
	jclass		JDBCUtilsClass;
	jstring		jtableName;
	jmethodID	idGetTypeConversionWarnings;
	jobjectArray jwarnings;
	char	  **warnings = NULL;
	int			warning_count;
	int			i;

	if (tableName == NULL)
		return NULL;

	jtableName = (*Jenv)->NewStringUTF(Jenv, tableName);

	/* Get JDBCUtils */
	PG_TRY();
	{
		jq_get_JDBCUtils(jdbcUtilsInfo, &JDBCUtilsClass, &JDBCUtilsObject);
	}
	PG_CATCH();
	{
		(*Jenv)->DeleteLocalRef(Jenv, jtableName);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Get getTypeConversionWarnings method */
	idGetTypeConversionWarnings = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass,
													   "getTypeConversionWarnings",
													   "(Ljava/lang/String;)[Ljava/lang/String;");
	if (idGetTypeConversionWarnings == NULL)
	{
		(*Jenv)->DeleteLocalRef(Jenv, jtableName);
		ereport(ERROR, (errmsg("Failed to find the JDBCUtils.getTypeConversionWarnings method")));
	}

	/* Call Java method */
	jq_exception_clear();
	jwarnings = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject,
										  idGetTypeConversionWarnings, jtableName);
	jq_get_exception();

	(*Jenv)->DeleteLocalRef(Jenv, jtableName);

	if (jwarnings == NULL)
		return NULL;

	/* Convert Java String array to C string array */
	warning_count = (*Jenv)->GetArrayLength(Jenv, jwarnings);

	if (warning_count == 0)
	{
		(*Jenv)->DeleteLocalRef(Jenv, jwarnings);
		return NULL;
	}

	/* Allocate array with NULL terminator */
	warnings = (char **) palloc0(sizeof(char *) * (warning_count + 1));

	for (i = 0; i < warning_count; i++)
	{
		jobject		jwarning = (*Jenv)->GetObjectArrayElement(Jenv, jwarnings, i);
		char	   *warning_str = jdbc_convert_string_to_cstring(jwarning);

		warnings[i] = pstrdup(warning_str);
	}

	warnings[warning_count] = NULL;	/* NULL terminator */

	(*Jenv)->DeleteLocalRef(Jenv, jwarnings);

	return warnings;
}

/*
 * jq_get_JDBCUtils: get JDBCUtilsClass and JDBCUtilsObject
 */
static void
jq_get_JDBCUtils(JDBCUtilsInfo * jdbcUtilsInfo, jclass * JDBCUtilsClass, jobject * JDBCUtilsObject)
{
	/* Check if jdbcUtilsInfo is valid before accessing it */
	if (jdbcUtilsInfo == NULL)
	{
		ereport(ERROR, (errmsg("jq_get_JDBCUtils: jdbcUtilsInfo is NULL")));
	}

	/* Check if Jenv is valid */
	if (Jenv == NULL)
	{
		ereport(ERROR, (errmsg("jq_get_JDBCUtils: JNI environment is NULL")));
	}

	elog(LOG, "jdbc_fdw: jq_get_JDBCUtils called with jdbcUtilsInfo=%p",
		 (void *)jdbcUtilsInfo);

	/*
	 * CRITICAL: Read JDBCUtilsObject EXACTLY ONCE from the malloc'd struct.
	 * Do not read it multiple times - causes corruption!
	 */
	jobject utils_obj = jdbcUtilsInfo->JDBCUtilsObject;
	*JDBCUtilsObject = utils_obj;

	if (*JDBCUtilsObject == NULL)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_ERROR),
			 errmsg("JDBCUtilsObject is NULL - connection was not properly initialized")));
	}

	/* Validate that the JNI reference is still valid */
	if ((*Jenv)->GetObjectRefType(Jenv, utils_obj) != JNIGlobalRefType)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_ERROR),
			 errmsg("JDBCUtilsObject is not a valid global reference")));
	}

	*JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");
	if (*JDBCUtilsClass == NULL)
	{
		ereport(ERROR, (errmsg("JDBCUtils class could not be created")));
	}
}

/*
 * jq_inval_callback
 *		After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * 	mark JDBC connections depending on that entry as needing to be remade.
 */
void
jq_inval_callback(int cacheid, uint32 hashvalue)
{
	jmethodID	callback = NULL;
	jclass		JDBCUtilsClass;

	/*
	 * CRITICAL: This function is called during error processing/cleanup!
	 * We must NOT call ereport(ERROR) here or we'll create infinite recursion
	 * causing ERRORDATA_STACK_SIZE exceeded panic. Fail silently instead.
	 */

	if (jvm == NULL || Jenv == NULL)
		return;

	JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");
	if (JDBCUtilsClass == NULL)
		return;  /* Fail silently - we're in error cleanup */

	/* hashvalue == 0 means a cache reset, must clear all state */
	if (hashvalue == 0)
	{
		/* release all connection */
		callback = (*Jenv)->GetStaticMethodID(Jenv, JDBCUtilsClass, "finalizeAllConns", "(J)V");
	}
	else if (cacheid == FOREIGNSERVEROID)
	{
		/* release connections of foreign server hashvalue */
		callback = (*Jenv)->GetStaticMethodID(Jenv, JDBCUtilsClass, "finalizeAllServerConns", "(J)V");
	}
	else if (cacheid == USERMAPPINGOID)
	{
		/* release connections of usermapping hashvalue */
		callback = (*Jenv)->GetStaticMethodID(Jenv, JDBCUtilsClass, "finalizeAllUserMapingConns", "(J)V");
	}

	if (callback == NULL)
		return;  /* Fail silently - we're in error cleanup, don't cascade errors */

	jq_exception_clear();
	(*Jenv)->CallStaticVoidMethod(Jenv, JDBCUtilsClass, callback, (jlong) hashvalue);

	/*
	 * CRITICAL: Don't call jq_get_exception() here! It calls ereport(ERROR)
	 * which would create infinite recursion. Just clear any exception silently.
	 */
	if ((*Jenv)->ExceptionCheck(Jenv))
		(*Jenv)->ExceptionClear(Jenv);

	/* Do NOT detach from JVM or set Jenv to NULL!
	 * The JVM should stay alive for the lifetime of the PostgreSQL backend.
	 * Detaching here causes JDBCUtilsObject references to become inaccessible.
	 */
}

/*
 * jdbc_reset_connection_cache
 * 		Reset all JDBC connections (for pgBouncer backend reuse)
 * 		This is called when we detect the backend has been reassigned
 */
void
jdbc_reset_connection_cache(void)
{
	/* This will be called from connection.c to reset state */
	elog(DEBUG1, "jdbc_fdw: Connection cache reset requested");

	/* Call the Java-side cleanup if needed */
	jq_release_all_result_sets();
}

/*
 * jq_release_all_result_sets
 *		release all cached result set
 */
void
jq_release_all_result_sets(void)
{
	jmethodID	methodId = NULL;
	jclass		JDBCUtilsClass;

	if (jvm == NULL)
		return;

	/* Ensure JVM is initialized */
	if (Jenv == NULL)
	{
		elog(WARNING, "jdbc_fdw: JNI environment not initialized for result set release");
		return;
	}

	JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");

	/* release all cached result set */
	methodId = (*Jenv)->GetStaticMethodID(Jenv, JDBCUtilsClass, "finalizeAllResultSet", "()V");

	jq_exception_clear();
	(*Jenv)->CallStaticVoidMethod(Jenv, JDBCUtilsClass, methodId);
	jq_get_exception();

	/* Do NOT detach from JVM or set Jenv to NULL!
	 * The JVM should stay alive for the lifetime of the PostgreSQL backend.
	 * Detaching here causes JDBCUtilsObject references to become inaccessible.
	 */
}
