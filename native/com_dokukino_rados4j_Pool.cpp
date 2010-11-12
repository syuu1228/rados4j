/*******************************************************************************
*Copyright (c) 2010  Takuya ASADA
* 
*  This program is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, only version 3 of the License.
* 
* 
*  This file is distributed in the hope that it will be useful, but WITHOUT
*  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
*  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
*  for more details.
* 
*  You should have received a copy of the GNU General Public License along
*  with this program.  If not, see <http://www.gnu.org/licenses/>.
* 
*  Please contact Eucalyptus Systems, Inc., 130 Castilian
*  Dr., Goleta, CA 93101 USA or visit <http://www.eucalyptus.com/licenses/>
*  if you need additional information or have any questions.
* 
*  This file may incorporate work covered under the following copyright and
*  permission notice:
* 
*    Software License Agreement (BSD License)
* 
*    Copyright (c) 2008, Regents of the University of California
*    All rights reserved.
* 
*    Redistribution and use of this software in source and binary forms, with
*    or without modification, are permitted provided that the following
*    conditions are met:
* 
*      Redistributions of source code must retain the above copyright notice,
*      this list of conditions and the following disclaimer.
* 
*      Redistributions in binary form must reproduce the above copyright
*      notice, this list of conditions and the following disclaimer in the
*      documentation and/or other materials provided with the distribution.
* 
*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
*    IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
*    TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
*    PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
*    OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
*    EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
*    PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
*    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
*    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
*    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
*    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. USERS OF
*    THIS SOFTWARE ACKNOWLEDGE THE POSSIBLE PRESENCE OF OTHER OPEN SOURCE
*    LICENSED MATERIAL, COPYRIGHTED MATERIAL OR PATENTED MATERIAL IN THIS
*    SOFTWARE, AND IF ANY SUCH MATERIAL IS DISCOVERED THE PARTY DISCOVERING
*    IT MAY INFORM DR. RICH WOLSKI AT THE UNIVERSITY OF CALIFORNIA, SANTA
*    BARBARA WHO WILL THEN ASCERTAIN THE MOST APPROPRIATE REMEDY, WHICH IN
*    THE REGENTS’ DISCRETION MAY INCLUDE, WITHOUT LIMITATION, REPLACEMENT
*    OF THE CODE SO IDENTIFIED, LICENSING OF THE CODE SO IDENTIFIED, OR
*    WITHDRAWAL OF THE CODE CAPABILITY TO THE EXTENT NEEDED TO COMPLY WITH
*    ANY SUCH LICENSES OR RIGHTS.
*******************************************************************************/
#include "com_dokukino_rados4j_Pool.h"
#include <rados/librados.hpp>
#include <iostream>

jclass pool_class = NULL, stat_class = NULL;
jfieldID pool_instance_ptr = NULL, pool_rados_ptr = NULL, stat_psize = NULL, stat_pmtime = NULL;
jmethodID pool_constructor = NULL, stat_constructor = NULL;

extern jclass list_ctx_class;
extern jmethodID list_ctx_constructor;
extern jfieldID list_ctx_instance_ptr, list_ctx_rados_ptr;

int
pool_field_initialize(JNIEnv *env)
{
    jclass clsj = env->FindClass("Lcom/dokukino/rados4j/Pool;");
    if (!clsj)
        return -1;
    pool_class = reinterpret_cast<jclass>(env->NewGlobalRef(clsj));
    env->DeleteLocalRef(clsj);

    pool_constructor = env->GetMethodID(pool_class, "<init>", "()V");
    if (!pool_constructor)
        return -1;
    pool_instance_ptr = env->GetFieldID(pool_class, "instancePtr", "J");
    if (!pool_instance_ptr)
        return -1;
    pool_rados_ptr = env->GetFieldID(pool_class, "radosPtr", "J");
    if (!pool_rados_ptr)
        return -1;
    return 0;
}

int
stat_field_initialize(JNIEnv *env)
{
    jclass clsj = env->FindClass("Lcom/dokukino/rados4j/Stat;");
    if (!clsj)
        return -1;
    stat_class = reinterpret_cast<jclass>(env->NewGlobalRef(clsj));
    env->DeleteLocalRef(clsj);

    stat_constructor = env->GetMethodID(stat_class, "<init>", "()V");
    if (!stat_constructor)
        return -1;
    stat_psize = env->GetFieldID(stat_class, "psize", "J");
    if (!stat_psize)
        return -1;
    stat_pmtime = env->GetFieldID(stat_class, "pmtime", "J");
    if (!stat_pmtime)
        return -1;
    return 0;
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

static inline librados::pool_t
pool_get_instance_ptr(JNIEnv *env, jobject obj)
{
    jlong ptr = env->GetLongField(obj, pool_instance_ptr);
    return reinterpret_cast<librados::pool_t>(ptr);
}

static inline librados::Rados *
pool_get_rados_ptr(JNIEnv *env, jobject obj)
{
    jlong ptr = env->GetLongField(obj, pool_rados_ptr);
    return reinterpret_cast<librados::Rados *>(ptr);
}

static inline jobject
pool_new_instance(JNIEnv *env)
{
    return env->NewObject(pool_class, pool_constructor);
}

static inline jobject
stat_new_instance(JNIEnv *env)
{
    return env->NewObject(stat_class, stat_constructor);
}

static inline void
stat_set_psize(JNIEnv *env, jobject obj, uint64_t psize)
{
    env->SetLongField(obj, stat_psize, static_cast<jlong>(psize));
}

static inline void
stat_set_pmtime(JNIEnv *env, jobject obj, time_t pmtime)
{
    env->SetLongField(obj, stat_pmtime, static_cast<jlong>(pmtime));
}

static inline jobject
list_ctx_new_instance(JNIEnv *env)
{
    return env->NewObject(list_ctx_class, list_ctx_constructor);
}

static inline void
list_ctx_set_instance_ptr(JNIEnv *env, jobject obj, librados::Rados::ListCtx *ctx)
{
    env->SetLongField(obj, list_ctx_instance_ptr, reinterpret_cast<jlong>(ctx));
}

static inline void
list_ctx_set_rados_ptr(JNIEnv *env, jobject obj, librados::Rados *rados)
{
    env->SetLongField(obj, list_ctx_rados_ptr, reinterpret_cast<jlong>(rados));
}

extern "C"
{

    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    delete
     * Signature: ()I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Pool_delete
    (JNIEnv *env, jobject obj)
    {
        librados::Rados *rados;
        librados::pool_t pool_ptr;

        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return -1;
        pool_ptr = pool_get_instance_ptr(env, obj);
        if (!pool_ptr)
            return -1;
        return rados->delete_pool(pool_ptr);
    }

    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    close
     * Signature: ()V
     */
    JNIEXPORT void JNICALL Java_com_dokukino_rados4j_Pool_close
    (JNIEnv *env, jobject obj)
    {
        librados::Rados *rados;
        librados::pool_t pool_ptr;

        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return;
        pool_ptr = pool_get_instance_ptr(env, obj);
        if (!pool_ptr)
            return;

        rados->close_pool(pool_ptr);
        pool_set_instance_ptr(env, obj, NULL);
    }

    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    createObj
     * Signature: (Ljava/lang/String;Z)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Pool_createObj
    (JNIEnv *env, jobject obj, jstring oid, jboolean exclusive)
    {
        librados::Rados *rados;
        librados::pool_t pool_ptr;
        const char *oid_char;
        jint ret;

        if (!oid)
            return -1;
        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return -1;
        pool_ptr = pool_get_instance_ptr(env, obj);
        if (!pool_ptr)
            return -1;
        oid_char = env->GetStringUTFChars(oid, NULL);
        if (!oid_char)
            return -1;
        ret = rados->create(pool_ptr, oid_char, exclusive);
        env->ReleaseStringUTFChars(oid, oid_char);
        return ret;
    }

    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    writeObj
     * Signature: (Ljava/lang/String;J[BJ)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Pool_writeObj
    (JNIEnv *env, jobject obj, jstring oid, jlong off, jbyteArray buf, jlong len)
    {
        librados::Rados *rados;
        librados::pool_t pool_ptr;
        librados::bufferlist bl;
        const char *oid_char;
        jbyte* src;
        jint ret;

        if (!oid || !buf)
            return -1;

        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return -1;

        pool_ptr = pool_get_instance_ptr(env, obj);
        if (!pool_ptr)
            return -1;

        oid_char = env->GetStringUTFChars(oid, NULL);
        if (!oid_char)
            return -1;

        src = env->GetByteArrayElements(buf, NULL);
        bl.append(reinterpret_cast<char *>(src), len);
        ret = rados->write(pool_ptr, oid_char, off, bl, len);
        env->ReleaseByteArrayElements(buf, src, 0);
        env->ReleaseStringUTFChars(oid, oid_char);

        return ret;
    }

    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    readObj
     * Signature: (Ljava/lang/String;J[BJ)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Pool_readObj
    (JNIEnv *env, jobject obj, jstring oid, jlong off, jbyteArray buf, jlong len)
    {
        librados::Rados *rados;
        librados::pool_t pool_ptr;
        librados::bufferlist bl;
        const char *oid_char;
        jint ret;

        if (!oid || !buf)
            return -1;
        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return -1;
        pool_ptr = pool_get_instance_ptr(env, obj);
        if (!pool_ptr)
            return -1;
        oid_char = env->GetStringUTFChars(oid, NULL);
        if (!oid_char)
            return -1;

        ret = rados->read(pool_ptr, oid_char, off, bl, len);

        if (ret > 0)
            env->SetByteArrayRegion(buf, 0, ret, reinterpret_cast<jbyte*>(bl.c_str()));
        env->ReleaseStringUTFChars(oid, oid_char);

        return ret;
    }


    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    renameObj
     * Signature: (Ljava/lang/String;Ljava/lang/String;)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Pool_renameObj
    (JNIEnv *env, jobject obj, jstring old_oid, jstring new_oid)
    {
        librados::Rados *rados;
        librados::pool_t pool_ptr;
        librados::bufferlist bl;
        const char *old_oid_char, *new_oid_char;
        jint ret;

        if (!old_oid || !new_oid)
            return -1;
        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return -1;
        pool_ptr = pool_get_instance_ptr(env, obj);
        if (!pool_ptr)
            return -1;
        old_oid_char = env->GetStringUTFChars(old_oid, NULL);
        if (!old_oid_char)
            return -1;
        new_oid_char = env->GetStringUTFChars(new_oid, NULL);
        if (!new_oid_char)
        {
            env->ReleaseStringUTFChars(old_oid, old_oid_char);
            return -1;
        }

        ret = rados->read(pool_ptr, old_oid_char, 0, bl, 0);
        if (ret < 0)
            goto err;

        ret = rados->write_full(pool_ptr, new_oid_char, bl);
        if (ret < 0)
            goto err;

        rados->remove(pool_ptr, old_oid_char);
err:
        env->ReleaseStringUTFChars(old_oid, old_oid_char);
        env->ReleaseStringUTFChars(new_oid, new_oid_char);
        return ret;
    }

    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    copyObj
     * Signature: (Ljava/lang/String;Lcom/dokukino/rados4j/Pool;Ljava/lang/String;)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Pool_copyObj
    (JNIEnv *env, jobject obj, jstring src_oid, jobject dest_pool, jstring dest_oid)
    {
        librados::Rados *rados;
        librados::pool_t src_pool_ptr, dest_pool_ptr;
        librados::bufferlist bl;
        const char *src_oid_char, *dest_oid_char;
        jint ret;

        if (!src_oid || !dest_pool || !dest_oid)
            return -1;
        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return -1;
        src_pool_ptr = pool_get_instance_ptr(env, obj);
        if (!src_pool_ptr)
            return -1;
        dest_pool_ptr = pool_get_instance_ptr(env, dest_pool);
        if (!dest_pool_ptr)
            return -1;
        src_oid_char = env->GetStringUTFChars(src_oid, NULL);
        if (!src_oid_char)
            return -1;
        dest_oid_char = env->GetStringUTFChars(dest_oid, NULL);
        if (!dest_oid_char)
        {
            env->ReleaseStringUTFChars(src_oid, src_oid_char);
            return -1;
        }

        ret = rados->read(src_pool_ptr, src_oid_char, 0, bl, 0);
        if (ret < 0)
            goto err;

        ret = rados->write_full(dest_pool_ptr, dest_oid_char, bl);
        if (ret < 0)
            goto err;

err:
        env->ReleaseStringUTFChars(src_oid, src_oid_char);
        env->ReleaseStringUTFChars(dest_oid, dest_oid_char);
        return ret;
    }


    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    removeObj
     * Signature: (Ljava/lang/String;)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_Pool_removeObj
    (JNIEnv *env, jobject obj, jstring oid)
    {
        librados::Rados *rados;
        librados::pool_t pool_ptr;
        const char *oid_char;
        jint ret;

        if (!oid)
            return -1;
        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return -1;
        pool_ptr = pool_get_instance_ptr(env, obj);
        if (!pool_ptr)
            return -1;
        oid_char = env->GetStringUTFChars(oid, NULL);
        if (!oid_char)
            return -1;
        ret = rados->remove(pool_ptr, oid_char);
        env->ReleaseStringUTFChars(oid, oid_char);

        return ret;
    }

    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    statObj
     * Signature: (Ljava/lang/String;)Lcom/dokukino/rados4j/Stat;
     */
    JNIEXPORT jobject JNICALL Java_com_dokukino_rados4j_Pool_statObj
    (JNIEnv *env, jobject obj, jstring oid)
    {
        librados::Rados *rados;
        librados::pool_t pool_ptr;
        const char *oid_char;
        uint64_t psize;
        time_t pmtime;
        jint ret;

        if (!oid)
            return NULL;

        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return NULL;

        pool_ptr = pool_get_instance_ptr(env, obj);
        if (!pool_ptr)
            return NULL;

        oid_char = env->GetStringUTFChars(oid, NULL);
        if (!oid_char)
            return NULL;

        ret = rados->stat(pool_ptr, oid_char, &psize, &pmtime);

        env->ReleaseStringUTFChars(oid, oid_char);
        if (ret < 0)
            return NULL;
        else
        {
            jobject obj = stat_new_instance(env);
            if (!obj)
                return NULL;
            stat_set_psize(env, obj, psize);
            stat_set_pmtime(env, obj, pmtime);
            return obj;
        }
    }

    /*
     * Class:     com_dokukino_rados4j_Pool
     * Method:    openList
     * Signature: ()Lcom/dokukino/rados4j/ListCtx;
     */
    JNIEXPORT jobject JNICALL Java_com_dokukino_rados4j_Pool_openList
    (JNIEnv *env, jobject obj)
    {
        librados::Rados *rados;
        librados::pool_t pool_ptr;
        librados::Rados::ListCtx *ctx = NULL;
        jint ret;

        rados = pool_get_rados_ptr(env, obj);
        if (!rados)
            return NULL;

        pool_ptr = pool_get_instance_ptr(env, obj);
        if (!pool_ptr)
            return NULL;

        ctx = new librados::Rados::ListCtx();
        ret = rados->list_objects_open(pool_ptr, ctx);

        if (ret < 0)
        {
            delete ctx;
            return NULL;
        }
        else
        {
            jobject obj = list_ctx_new_instance(env);
            if (!obj)
                return NULL;
            list_ctx_set_instance_ptr(env, obj, ctx);
            list_ctx_set_rados_ptr(env, obj, rados);
            return obj;
        }
    }
}
