#include "com_dokukino_rados4j_Rados.h"
#include <rados/librados.hpp>
#include <iostream>

static jfieldID rados_instance_ptr = NULL;
static const char **rados_argv = NULL;
static int rados_argc = 0;
static jstring *rados_strs = NULL;
static jobjectArray rados_args = NULL;

extern jclass pool_class;
extern jfieldID pool_instance_ptr, pool_rados_ptr;
extern jmethodID pool_constructor;

extern int pool_field_initialize(JNIEnv *);
extern int stat_field_initialize(JNIEnv *);
extern int list_ctx_field_initialize(JNIEnv *);

static int
rados_field_initialize(JNIEnv *env)
{
    jclass clsj = env->FindClass("Lcom/dokukino/rados4j/Rados;");
    if (!clsj)
    {
        printf("findclass failed\n");
        return -1;
    }
    rados_instance_ptr = env->GetFieldID(clsj, "instancePtr", "J");
    if (!rados_instance_ptr)
    {
        printf("getfieldid failed\n");
        return -1;
    }
    return 0;
}

static inline void
rados_set_instance_ptr(JNIEnv *env, jobject obj, librados::Rados *rados)
{
    env->SetLongField(obj, rados_instance_ptr, reinterpret_cast<jlong>(rados));
}

static inline librados::Rados *
rados_get_instance_ptr(JNIEnv *env, jobject obj)
{
    jlong ptr = env->GetLongField(obj, rados_instance_ptr);
    return reinterpret_cast<librados::Rados *>(ptr);
}

static inline jobject
pool_new_instance(JNIEnv *env)
{
    return env->NewObject(pool_class, pool_constructor);
}

static inline void
pool_set_instance_ptr(JNIEnv *env, jobject obj, librados::pool_t pool)
{
    env->SetLongField(obj, pool_instance_ptr, reinterpret_cast<jlong>(pool));
}

static inline void
pool_set_rados_ptr(JNIEnv *env, jobject obj, librados::Rados *rados)
{
    env->SetLongField(obj, pool_rados_ptr, reinterpret_cast<jlong>(rados));
}

extern "C"
{
    /*
     * Class:     com_dokukino_rados4j_Rados
     * Method:    initialize
     * Signature: ([Ljava/lang/String;)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Rados_initialize
    (JNIEnv *env, jobject obj, jobjectArray args)
    {
        jint ret;
        librados::Rados *rados;

        ret = rados_field_initialize(env);
        if (ret)
            return -1;

        ret = pool_field_initialize(env);
        if (ret)
            return -1;

        ret = stat_field_initialize(env);
        if (ret)
            return -1;

        ret = list_ctx_field_initialize(env);
        if (ret)
            return -1;

        rados = new librados::Rados();
        if (!rados)
            return -1;

	rados_args = args;
	rados_argc = env->GetArrayLength(args);
	if (rados_argc > 0) {
		rados_argv = new const char*[rados_argc];
		rados_strs = new jstring[rados_argc];
		for (int i = 0; i < rados_argc; i++) {
			rados_strs[i] = (jstring)env->GetObjectArrayElement(args, i);
			rados_argv[i] = env->GetStringUTFChars(rados_strs[i], NULL);
			printf("[%d] %s\n", i, rados_argv[i]);
		}
	        ret = rados->initialize(rados_argc, rados_argv);
	}else
	        ret = rados->initialize(0, NULL);
        rados_set_instance_ptr(env, obj, rados);
        return ret;
    }

    /*
     * Class:     com_dokukino_rados4j_Rados
     * Method:    shutdown
     * Signature: ()V
     */
    JNIEXPORT void JNICALL Java_com_dokukino_rados4j_Rados_shutdown
    (JNIEnv *env, jobject obj)
    {
        librados::Rados *rados;

        rados = rados_get_instance_ptr(env, obj);
        if (!rados)
            return;
        rados->shutdown();
        delete rados;
	rados_set_instance_ptr(env, obj, NULL);
	if (rados_argv) {
		for (int i = 0; i < rados_argc; i++) {
			env->ReleaseStringUTFChars(rados_strs[i], rados_argv[i]);
		}
		delete rados_argv;
		delete rados_strs;
	}
    }

    /*
     * Class:     com_dokukino_rados4j_Rados
     * Method:    openPool
     * Signature: (Ljava/lang/String;Lcom/dokukino/rados4j/Pool;)I
     */
    JNIEXPORT jobject JNICALL Java_com_dokukino_rados4j_Rados_openPool
    (JNIEnv *env, jobject obj, jstring name)
    {
        librados::Rados *rados;
        const char *name_char;
        librados::pool_t pool;
        jint ret;

        if (!name)
            return NULL;
        rados = rados_get_instance_ptr(env, obj);
        if (!rados)
            return NULL;
        name_char = env->GetStringUTFChars(name, NULL);
        if (!name_char)
            return NULL;
        ret = rados->open_pool(name_char, &pool);

        env->ReleaseStringUTFChars(name, name_char);
        if (ret < 0)
            return NULL;
        else
        {
            jobject obj = pool_new_instance(env);
            if (!obj)
            {
                rados->close_pool(pool);
                return NULL;
            }
            pool_set_instance_ptr(env, obj, pool);
            pool_set_rados_ptr(env, obj, rados);
            return obj;
        }
    }

    /*
     * Class:     com_dokukino_rados4j_Rados
     * Method:    lookupPool
     * Signature: (Ljava/lang/String;)Z
     */
    JNIEXPORT jboolean JNICALL Java_com_dokukino_rados4j_Rados_lookupPool
    (JNIEnv *env, jobject obj, jstring name)
    {
        librados::Rados *rados;
        const char *name_char;
        int ret;

        if (!name)
            return -1;

        rados = rados_get_instance_ptr(env, obj);
        if (!rados)
            return -1;

        name_char = env->GetStringUTFChars(name, NULL);
        if (!name_char)
            return -1;

        ret = rados->lookup_pool(name_char);
        env->ReleaseStringUTFChars(name, name_char);

        return (ret > 0);
    }

    /*
     * Class:     com_dokukino_rados4j_Rados
     * Method:    listPools
     * Signature: ([Ljava/lang/String;)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Rados_listPools
    (JNIEnv *env, jobject obj, jobjectArray entries)
    {
        librados::Rados *rados;
        std::list<std::string> entries_list;
        int r;

        if (!entries)
            return -1;

        rados = rados_get_instance_ptr(env, obj);
        if (!rados)
            return -1;

        r = rados->list_pools(entries_list);
        if (r < 0)
            return r;

        int i = 0;
        const int len = env->GetArrayLength(entries);
        for (std::list<std::string>::iterator iter = entries_list.begin();
                i < len && iter != entries_list.end(); ++iter, ++i)
        {
            jstring entry = env->NewStringUTF((*iter).c_str());
            env->SetObjectArrayElement(entries, i, entry);
        }

        return r;
    }

    /*
     * Class:     com_dokukino_rados4j_Rados
     * Method:    createPool
     * Signature: (Ljava/lang/String;J)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Rados_createPool
    (JNIEnv *env, jobject obj, jstring name, jlong auid)
    {
        librados::Rados *rados;
        const char *name_char;

        if (!name)
            return -1;
        rados = rados_get_instance_ptr(env, obj);
        if (!rados)
            return -1;
        name_char = env->GetStringUTFChars(name, NULL);
        if (!name_char)
            return -1;
        return rados->create_pool(name_char, auid);
    }

}
