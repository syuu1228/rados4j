#include "com_dokukino_rados4j_ListCtx.h"
#include <rados/librados.hpp>
#include <list>

jclass list_ctx_class = NULL;
jmethodID list_ctx_constructor = NULL;
jfieldID list_ctx_instance_ptr = NULL, list_ctx_rados_ptr = NULL;

int
list_ctx_field_initialize(JNIEnv *env)
{
    jclass clsj = env->FindClass("Lcom/dokukino/rados4j/ListCtx;");
    if (!clsj)
        return -1;
    list_ctx_class = reinterpret_cast<jclass>(env->NewGlobalRef(clsj));
    env->DeleteLocalRef(clsj);
    list_ctx_constructor = env->GetMethodID(list_ctx_class, "<init>", "()V");
    if (!list_ctx_constructor)
        return -1;
    list_ctx_instance_ptr = env->GetFieldID(list_ctx_class, "instancePtr", "J");
    if (!list_ctx_instance_ptr)
        return -1;
    list_ctx_rados_ptr = env->GetFieldID(list_ctx_class, "radosPtr", "J");
    if (!list_ctx_rados_ptr)
        return -1;
    return 0;
}

static inline void
list_ctx_set_instance_ptr(JNIEnv *env, jobject obj, librados::Rados::ListCtx *ptr)
{
    env->SetLongField(obj, list_ctx_instance_ptr, reinterpret_cast<long>(ptr));
}

static inline librados::Rados::ListCtx *
list_ctx_get_instance_ptr(JNIEnv *env, jobject obj)
{
    jlong ptr = env->GetLongField(obj, list_ctx_instance_ptr);
    return reinterpret_cast<librados::Rados::ListCtx *>(ptr);
}

static inline librados::Rados *
list_ctx_get_rados_ptr(JNIEnv *env, jobject obj)
{
    jlong ptr = env->GetLongField(obj, list_ctx_rados_ptr);
    return reinterpret_cast<librados::Rados *>(ptr);
}

extern "C"
{
    /*
     * Class:     com_dokukino_rados4j_ListCtx
     * Method:    more
     * Signature: (I[Ljava/lang/String;)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_ListCtx_more
    (JNIEnv *env, jobject obj, jint max, jobjectArray entries)
    {
        librados::Rados::ListCtx *ctx;
        librados::Rados *rados;
        std::list<std::string> entries_list;
        jint r;

        if (!entries)
            return -1;
        ctx = list_ctx_get_instance_ptr(env, obj);
        if (!ctx)
            return -1;
        rados = list_ctx_get_rados_ptr(env, obj);
        if (!rados)
            return -1;
        r = rados->list_objects_more(*ctx, max, entries_list);
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
     * Class:     com_dokukino_rados4j_ListCtx
     * Method:    close
     * Signature: ()V
     */
    JNIEXPORT void JNICALL Java_com_dokukino_rados4j_ListCtx_close
    (JNIEnv *env, jobject obj)
    {
        librados::Rados::ListCtx *ctx;
        librados::Rados *rados;

        ctx = list_ctx_get_instance_ptr(env, obj);
        if (!ctx)
            return;
        rados = list_ctx_get_rados_ptr(env, obj);
        if (!rados)
            return;
        rados->list_objects_close(*ctx);
        delete ctx;
	list_ctx_set_instance_ptr(env, obj, NULL);
    }

}
