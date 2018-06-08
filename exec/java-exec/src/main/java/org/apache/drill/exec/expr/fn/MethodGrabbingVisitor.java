/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.fn;

import java.io.StringWriter;
import java.util.Map;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.MethodDeclarator;
import org.codehaus.janino.Visitor;
import org.codehaus.janino.util.Traverser;

import com.google.common.collect.Maps;


public class MethodGrabbingVisitor{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MethodGrabbingVisitor.class);

  private Class<?> c;
  private Map<String, String> methods = Maps.newHashMap();
  private ClassFinder<Object, Throwable> classFinder = new ClassFinder<>();
  private boolean captureMethods = false;

  private MethodGrabbingVisitor(Class<?> c) {
    super();
    this.c = c;
  }

  public class ClassFinder<R, EX extends Throwable> extends Traverser implements Visitor.TypeDeclarationVisitor<R, EX> {

    @Override
    public void traverseClassDeclaration(Java.AbstractClassDeclaration cd) {
//      logger.debug("Traversing: {}", cd.getClassName());
      boolean prevCapture = captureMethods;
      captureMethods = c.getName().equals(cd.getClassName());
      try {
        super.traverseClassDeclaration((Java.AbstractClassDeclaration)cd);
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      captureMethods = prevCapture;
    }

    @Override
    public void traverseMethodDeclarator(MethodDeclarator md) {
//      logger.debug(c.getName() + ": Found {}, include {}", md.name, captureMethods);

      if(captureMethods){
        StringWriter writer = new StringWriter();
        ModifiedUnparseVisitor v = new ModifiedUnparseVisitor(writer);
//      UnparseVisitor v = new UnparseVisitor(writer);

        md.accept(v);
        v.close();
        writer.flush();
        methods.put(md.name, writer.getBuffer().toString());
      }
    }

    @Override
    public R visitAnonymousClassDeclaration(Java.AnonymousClassDeclaration acd) throws EX {
      try {
        traverseClassDeclaration(acd);
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      return null;
    }

    @Override
    public R visitLocalClassDeclaration(Java.LocalClassDeclaration lcd) throws EX {
      try {
        traverseClassDeclaration(lcd);
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      return null;
    }

    @Override
    public R visitPackageMemberClassDeclaration(Java.AbstractPackageMemberClassDeclaration apmcd) throws EX {
      try {
        traverseClassDeclaration(apmcd);
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      return null;
    }

    @Override
    public R visitMemberInterfaceDeclaration(Java.MemberInterfaceDeclaration mid) throws EX {
//      try {
//        traverseClassDeclaration((ClassDeclaration) mid);
//      } catch (Throwable throwable) {
//        throwable.printStackTrace();
//      }
      return null;
    }

    @Override
    public R visitPackageMemberInterfaceDeclaration(Java.PackageMemberInterfaceDeclaration pmid) throws EX {
//      try {
//        traverseClassDeclaration((ClassDeclaration) pmid);
//      } catch (Throwable throwable) {
//        throwable.printStackTrace();
//      }
      return null;
    }

    @Override
    public R visitMemberClassDeclaration(Java.MemberClassDeclaration mcd) throws EX {
      try {
        traverseClassDeclaration(mcd);
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      return null;
    }

    @Override
    public R visitEnumConstant(Java.EnumConstant ec) throws EX {
      try {
        traverseClassDeclaration(ec);
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      return null;
    }

    @Override
    public R visitMemberEnumDeclaration(Java.MemberEnumDeclaration med) throws EX {
      try {
        traverseClassDeclaration(med);
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      return null;
    }

    @Override
    public R visitPackageMemberEnumDeclaration(Java.PackageMemberEnumDeclaration pmed) throws EX {
      try {
        traverseClassDeclaration(pmed);
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      return null;
    }

    @Override
    public R visitMemberAnnotationTypeDeclaration(Java.MemberAnnotationTypeDeclaration matd) throws EX {

      return null;
    }

    @Override
    public R visitPackageMemberAnnotationTypeDeclaration(Java.PackageMemberAnnotationTypeDeclaration pmatd) throws EX {
      return null;
    }
  }


  public static Map<String, String> getMethods(Java.CompilationUnit cu, Class<?> c){
    MethodGrabbingVisitor visitor = new MethodGrabbingVisitor(c);
    try {
      cu.getPackageMemberTypeDeclarations()[0].accept(visitor.classFinder);
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
    return visitor.methods;
  }

}
