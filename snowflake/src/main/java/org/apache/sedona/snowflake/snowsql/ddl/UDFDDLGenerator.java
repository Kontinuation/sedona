package org.apache.sedona.snowflake.snowsql.ddl;

import org.apache.sedona.snowflake.snowsql.UDFs;
import org.apache.sedona.snowflake.snowsql.annotations.UDFAnnotations;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class UDFDDLGenerator {

    public static Method[] udfMethods() {
        return UDFs.class.getDeclaredMethods();
    }

    public static String buildUDFDDL(Method method, Map<String, String> configs) {
        if (!method.isAnnotationPresent(UDFAnnotations.ParamMeta.class)) {
            throw new RuntimeException("Missing ParamMeta annotation for method: " + method.getName());
        }
        String[] args = method.getAnnotation(UDFAnnotations.ParamMeta.class).argNames();
        Class<?>[] argTypes = method.getParameterTypes();
        // generate argspec
        String argSpec = "";
        for (int i = 0;i < args.length; i++) {
            String argName = args[i].toLowerCase();
            try {
                String argType= Constants.snowflakeTypeMap.get(argTypes[i].getTypeName()).toUpperCase();
                argSpec = argSpec + argName + " " + argType;
                if (i + 1 != args.length) {
                    argSpec = argSpec + ", ";
                }
            } catch (NullPointerException e) {
                throw new RuntimeException("Unsupported type: " + argTypes[i].getTypeName());
            }
        }
        // generate return type
        String returnType = Constants.snowflakeTypeMap.get(method.getReturnType().getTypeName());
        if (returnType == null) {
            throw new RuntimeException("Unsupported type: " + method.getReturnType().getTypeName());
        }
        String handlerName = UDFs.class.getPackage().getName() + "." + UDFs.class.getSimpleName() + "." + method.getName();
        // check some function attributes
        String null_input_conf = method.isAnnotationPresent(UDFAnnotations.CallOnNull.class) ? "CALLED ON NULL INPUT" : "RETURNS NULL ON NULL INPUT";
        String immutable_conf = method.isAnnotationPresent(UDFAnnotations.Immutable.class) ? "IMMUTABLE" : "VOLATILE";
        return formatUDFDDL(
                method.getName(),
                configs.getOrDefault("schema", "sedona"),
                argSpec,
                returnType,
                "WHEROBOTS",
                handlerName,
                configs.get(Constants.SEDONA_VERSION),
                configs.get(Constants.GEOTOOLS_VERSION),
                null_input_conf,
                immutable_conf
        );
    }

    public static List<String> buildAll(Map<String, String> configs) {
        List<String> ddlList = new ArrayList<>();
        for (Method method : udfMethods()) {
            if (method.getModifiers() == (Modifier.PUBLIC | Modifier.STATIC)) {
                ddlList.add(buildUDFDDL(method, configs));
            }
        }
        return ddlList;
    }

    public static String formatUDFDDL(
            String functionName,
            String schemaName,
            String argSpec,
            String returnType,
            String stageName,
            String handlerName,
            String sedona_version,
            String geotools_version,
            String null_input_conf,
            String immutable_conf
    ) {
        String ddlTemplate = String.join("\n", new BufferedReader(
                new InputStreamReader(
                        Objects.requireNonNull(DDLGenerator.class.getClassLoader().getResourceAsStream("UDFTemplate.txt"))
                )
        ).lines().collect(Collectors.toList()));
        return ddlTemplate.replace(
                "{KW_FUNCTION_NAME}", functionName
        ).replace(
                "{KW_SCHEMA_NAME}", schemaName
        ).replace(
                "{KW_ARG_SPEC}", argSpec
        ).replace(
                "{KW_RETURN_TYPE}", returnType
        ).replace(
                "{KW_STAGE_NAME}", stageName
        ).replace(
                "{KW_HANDLER_NAME}", handlerName
        ).replace(
                "{KW_SEDONA_VERSION}", sedona_version
        ).replace(
                "{KW_GEOTOOLS_VERSION}", geotools_version
        ).replace(
                "{KW_NULL_INPUT_CONF}", null_input_conf
        ).replace(
                "{KW_IMMUTABLE_CONF}", immutable_conf
        );
    }

}
