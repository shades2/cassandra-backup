package com.instaclustr.cassandra.backup.impl.restore;

import static java.lang.String.format;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.nio.file.Files;

import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy.RestorationStrategyType;
import com.instaclustr.kubernetes.KubernetesHelper;

@Target({TYPE, PARAMETER})
@Retention(RUNTIME)
@Constraint(validatedBy = {
    ValidRestoreOperationRequest.RestoreOperationRequestValidator.class,
})
public @interface ValidRestoreOperationRequest {

    String message() default "{com.instaclustr.cassandra.backup.impl.backup.ValidRestoreOperationRequest.RestoreOperationRequestValidator.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    final class RestoreOperationRequestValidator implements ConstraintValidator<ValidRestoreOperationRequest, RestoreOperationRequest> {

        @Override
        public boolean isValid(final RestoreOperationRequest value, final ConstraintValidatorContext context) {

            context.disableDefaultConstraintViolation();

            if (value.restorationStrategyType == RestorationStrategyType.UNKNOWN) {
                context.buildConstraintViolationWithTemplate("restorationStrategyType is not recognized").addConstraintViolation();
                return false;
            }

            if (value.restorationPhase == RestorationPhaseType.UNKNOWN) {
                context.buildConstraintViolationWithTemplate("restorationPhase is not recognized").addConstraintViolation();
                return false;
            }

            if (value.restorationStrategyType == RestorationStrategyType.IMPORT) {
                if (value.importing == null) {
                    context.buildConstraintViolationWithTemplate("you can not specify IMPORTING restorationStrategyType and have 'import' field empty!");
                    return false;
                }
            }

            if (!Files.exists(value.cassandraDirectory)) {
                context.buildConstraintViolationWithTemplate(format("cassandraDirectory %s does not exist", value.cassandraDirectory)).addConstraintViolation();
                return false;
            }

            if ((KubernetesHelper.isRunningInKubernetes() || KubernetesHelper.isRunningAsClient()) && value.k8sBackupSecretName == null) {
                context.buildConstraintViolationWithTemplate("This code is running in Kubernetes or as a Kubernetes client "
                                                                 + "but there is not 'k8sSecretName' field set on backup request!").addConstraintViolation();
                return false;
            }

            return true;
        }
    }
}
