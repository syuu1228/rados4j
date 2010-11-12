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
#include "com_dokukino_rados4j_Rados.h"
#include <rados/librados.hpp>
#include <iostream>

static jfieldID rados_instance_ptr = NULL;

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
