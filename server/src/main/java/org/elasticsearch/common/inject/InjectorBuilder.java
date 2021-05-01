/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.BindingImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.internal.Stopwatch;
import org.elasticsearch.common.inject.spi.Dependency;

import java.util.List;

/**
 * Builds a tree of injectors. This is a primary injector, plus child injectors needed for each
 * {@link Binder#newPrivateBinder() private environment}. The primary injector is not necessarily a
 * top-level injector.
 * <p>
 * Injector construction happens in two phases.
 * <ol>
 * <li>Static building. In this phase, we interpret commands, create bindings, and inspect
 * dependencies. During this phase, we hold a lock to ensure consistency with parent injectors.
 * No user code is executed in this phase.</li>
 * <li>Dynamic injection. In this phase, we call user code. We inject members that requested
 * injection. This may require user's objects be created and their providers be called. And we
 * create eager singletons. In this phase, user code may have started other threads. This phase
 * is not executed for injectors created using {@link Stage#TOOL the tool stage}</li>
 * </ol>
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * 构建一个注入器树。这是一个主注入器，加上每个{@link Binder#newPrivateBinder() private environment}所需要的子注入器。
 * 主注入器不一定是顶层注入器。
 *
 * 注入器的建设分为两个阶段。
 *
 * 静态的building。在这个阶段，我们解释命令、创建绑定和检查依赖项。在此阶段，我们持有一个锁，以确保与父注入器的一致性。
 * 在此阶段不执行任何用户代码。
 *
 * 动态的注入。在这个阶段，我们称之为用户代码。我们注入请求注入的成员。这可能需要创建用户的对象并调用它们的提供者。
 * 我们创造了渴望单身的人。在此阶段，用户代码可能已经启动了其他线程。这个阶段不会被使用{@link Stage#TOOL the TOOL Stage}
 * 创建的注入器执行。
 *
 */
class InjectorBuilder {

    private final Stopwatch stopwatch = new Stopwatch();
    private final Errors errors = new Errors();

    private Stage stage;

    private final Initializer initializer = new Initializer();
    private final BindingProcessor bindingProcesor;
    private final InjectionRequestProcessor injectionRequestProcessor;

    private final InjectorShell.Builder shellBuilder = new InjectorShell.Builder();
    private List<InjectorShell> shells;

    InjectorBuilder() {
        injectionRequestProcessor = new InjectionRequestProcessor(errors, initializer);
        bindingProcesor = new BindingProcessor(errors, initializer);
    }

    /**
     * Sets the stage for the created injector. If the stage is {@link Stage#PRODUCTION}, this class
     * will eagerly load singletons.
     */
    InjectorBuilder stage(Stage stage) {
        shellBuilder.stage(stage);
        this.stage = stage;
        return this;
    }

    InjectorBuilder addModules(Iterable<? extends Module> modules) {
        shellBuilder.addModules(modules);
        return this;
    }

    Injector build() {
        if (shellBuilder == null) {
            throw new AssertionError("Already built, builders are not reusable.");
        }

        // Synchronize while we're building up the bindings and other injector state. This ensures that
        // the JIT bindings in the parent injector don't change while we're being built
        synchronized (shellBuilder.lock()) {
            shells = shellBuilder.build(initializer, bindingProcesor, stopwatch, errors);
            stopwatch.resetAndLog("Injector construction");

            initializeStatically();
        }

        injectDynamically();

        return primaryInjector();
    }

    /**
     * Initialize and validate everything.
     */
    private void initializeStatically() {
        bindingProcesor.initializeBindings();
        stopwatch.resetAndLog("Binding initialization");

        for (InjectorShell shell : shells) {
            shell.getInjector().index();
        }
        stopwatch.resetAndLog("Binding indexing");

        injectionRequestProcessor.process(shells);
        stopwatch.resetAndLog("Collecting injection requests");

        bindingProcesor.runCreationListeners();
        stopwatch.resetAndLog("Binding validation");

        injectionRequestProcessor.validate();
        stopwatch.resetAndLog("Static validation");

        initializer.validateOustandingInjections(errors);
        stopwatch.resetAndLog("Instance member validation");

        new LookupProcessor(errors).process(shells);
        for (InjectorShell shell : shells) {
            ((DeferredLookups) shell.getInjector().lookups).initialize(errors);
        }
        stopwatch.resetAndLog("Provider verification");

        for (InjectorShell shell : shells) {
            if (!shell.getElements().isEmpty()) {
                throw new AssertionError("Failed to execute " + shell.getElements());
            }
        }

        errors.throwCreationExceptionIfErrorsExist();
    }

    /**
     * Returns the injector being constructed. This is not necessarily the root injector.
     */
    private Injector primaryInjector() {
        return shells.get(0).getInjector();
    }

    /**
     * Inject everything that can be injected. This method is intentionally not synchronized. If we
     * locked while injecting members (ie. running user code), things would deadlock should the user
     * code build a just-in-time binding from another thread.
     */
    private void injectDynamically() {
        injectionRequestProcessor.injectMembers();
        stopwatch.resetAndLog("Static member injection");

        initializer.injectAll(errors);
        stopwatch.resetAndLog("Instance injection");
        errors.throwCreationExceptionIfErrorsExist();

        for (InjectorShell shell : shells) {
            loadEagerSingletons(shell.getInjector(), stage, errors);
        }
        stopwatch.resetAndLog("Preloading singletons");
        errors.throwCreationExceptionIfErrorsExist();
    }

    /**
     * Loads eager singletons, or all singletons if we're in Stage.PRODUCTION. Bindings discovered
     * while we're binding these singletons are not be eager.
     */
    public void loadEagerSingletons(InjectorImpl injector, Stage stage, Errors errors) {
        for (final Binding<?> binding : injector.state.getExplicitBindingsThisLevel().values()) {
            loadEagerSingletons(injector, stage, errors, (BindingImpl<?>)binding);
        }
        for (final Binding<?> binding : injector.jitBindings.values()) {
            loadEagerSingletons(injector, stage, errors, (BindingImpl<?>)binding);
        }
    }

    private void loadEagerSingletons(InjectorImpl injector, Stage stage, final Errors errors, BindingImpl<?> binding) {
        if (binding.getScoping().isEagerSingleton(stage)) {
            try {
                injector.callInContext(new ContextualCallable<Void>() {
                    Dependency<?> dependency = Dependency.get(binding.getKey());

                    @Override
                    public Void call(InternalContext context) {
                        context.setDependency(dependency);
                        Errors errorsForBinding = errors.withSource(dependency);
                        try {
                            binding.getInternalFactory().get(errorsForBinding, context, dependency);
                        } catch (ErrorsException e) {
                            errorsForBinding.merge(e.getErrors());
                        } finally {
                            context.setDependency(null);
                        }

                        return null;
                    }
                });
            } catch (ErrorsException e) {
                throw new AssertionError();
            }
        }
    }
}
