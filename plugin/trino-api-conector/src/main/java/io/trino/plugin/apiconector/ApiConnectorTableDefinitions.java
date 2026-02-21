/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.apiconector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.apiconector.ApiTableDefinition.ApiColumnDefinition;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;

public final class ApiConnectorTableDefinitions
{
    private static final Type VARCHAR = createUnboundedVarcharType();

    private static final Map<String, ApiTableDefinition> TABLES = buildDefinitions();

    private ApiConnectorTableDefinitions() {}

    public static List<ApiTableDefinition> listTables()
    {
        return ImmutableList.copyOf(TABLES.values());
    }

    public static ApiTableDefinition getTable(String tableName)
    {
        return TABLES.get(tableName.toLowerCase(ENGLISH));
    }

    private static Map<String, ApiTableDefinition> buildDefinitions()
    {
        ImmutableMap.Builder<String, ApiTableDefinition> tables = ImmutableMap.builder();

        tables.put("lista_lead", createListaLead());
        tables.put("lista_protocolo", createListaProtocolo());
        tables.put("lista_protocolo_status", createListaProtocoloStatus());
        tables.put("lista_atendimento", createListaAtendimento());
        tables.put("lista_contato_chat", createListaContatoChat());
        tables.put("lista_atendimento_chat", createListaAtendimentoChat());
        tables.put("lista_mensagem_atendimento_chat", createListaMensagemAtendimentoChat());
        tables.put("lista_atendimento_chatbot", createListaAtendimentoChatbot());
        tables.put("lista_mensagem_atendimento_chatbot", createListaMensagemAtendimentoChatbot());

        return tables.buildOrThrow();
    }

    private static ApiTableDefinition createListaLead()
    {
        ImmutableList.Builder<ApiColumnDefinition> columns = ImmutableList.builder();
        columns.add(new ApiColumnDefinition("id_lead", BIGINT));
        columns.add(new ApiColumnDefinition("dt_criacao", TIMESTAMP_MILLIS));
        columns.add(new ApiColumnDefinition("dt_modificacao", TIMESTAMP_MILLIS));
        columns.add(new ApiColumnDefinition("tp_entrada", BIGINT));
        columns.add(new ApiColumnDefinition("id_usuario", BIGINT));
        columns.add(new ApiColumnDefinition("id_cliente", BIGINT));
        columns.add(new ApiColumnDefinition("id_funil", BIGINT));
        columns.add(new ApiColumnDefinition("ds_chave", VARCHAR));
        columns.add(new ApiColumnDefinition("id_marca", BIGINT));
        columns.add(new ApiColumnDefinition("id_unidade", BIGINT));
        columns.add(new ApiColumnDefinition("fl_consultado", BOOLEAN));
        columns.add(new ApiColumnDefinition("fl_higienizado", BOOLEAN));
        columns.add(new ApiColumnDefinition("dt_higienizado", TIMESTAMP_MILLIS));
        columns.add(new ApiColumnDefinition("ds_lead_migrado", VARCHAR));

        for (int index = 1; index <= 160; index++) {
            columns.add(new ApiColumnDefinition("r" + index, VARCHAR));
        }
        columns.add(new ApiColumnDefinition("raw_json", VARCHAR));

        return new ApiTableDefinition(
                "lista_lead",
                "lista-lead",
                "dt_criacao",
                columns.build());
    }

    private static ApiTableDefinition createListaProtocolo()
    {
        return new ApiTableDefinition(
                "lista_protocolo",
                "lista-protocolo",
                "dt_criacao",
                withRawJson(ImmutableList.of(
                        new ApiColumnDefinition("id_protocoloatendimento", BIGINT),
                        new ApiColumnDefinition("dt_criacao", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("dt_modificacao", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("tp_entrada", BIGINT),
                        new ApiColumnDefinition("id_lead", BIGINT),
                        new ApiColumnDefinition("ds_protocolo", VARCHAR),
                        new ApiColumnDefinition("id_departamento", BIGINT),
                        new ApiColumnDefinition("id_motivoprotocolo", BIGINT))));
    }

    private static ApiTableDefinition createListaProtocoloStatus()
    {
        return new ApiTableDefinition(
                "lista_protocolo_status",
                "lista-protocolo-status",
                "dt_criacao",
                withRawJson(ImmutableList.of(
                        new ApiColumnDefinition("id_protocoloatendimentostatus", BIGINT),
                        new ApiColumnDefinition("id_protocoloatendimento", BIGINT),
                        new ApiColumnDefinition("dt_criacao", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("dt_modificacao", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("id_usuario", BIGINT),
                        new ApiColumnDefinition("fl_ativo", BOOLEAN),
                        new ApiColumnDefinition("tp_status", BIGINT))));
    }

    private static ApiTableDefinition createListaAtendimento()
    {
        return new ApiTableDefinition(
                "lista_atendimento",
                "lista-atendimento",
                "dt_criacao",
                withRawJson(ImmutableList.of(
                        new ApiColumnDefinition("id_atendimentolead", BIGINT),
                        new ApiColumnDefinition("dt_criacao", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("id_cliente", BIGINT),
                        new ApiColumnDefinition("tp_atendimento", BIGINT),
                        new ApiColumnDefinition("id_usuario", BIGINT),
                        new ApiColumnDefinition("id_funil", BIGINT),
                        new ApiColumnDefinition("id_lead", BIGINT),
                        new ApiColumnDefinition("id_atendimentochat", BIGINT),
                        new ApiColumnDefinition("id_atendimentochatbot", BIGINT),
                        new ApiColumnDefinition("ds_observacao", VARCHAR))));
    }

    private static ApiTableDefinition createListaContatoChat()
    {
        return new ApiTableDefinition(
                "lista_contato_chat",
                "lista-contato-chat",
                "dt_cadastro",
                withRawJson(ImmutableList.of(
                        new ApiColumnDefinition("id_contato", BIGINT),
                        new ApiColumnDefinition("dt_cadastro", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("id_cliente", BIGINT),
                        new ApiColumnDefinition("id_lead", BIGINT),
                        new ApiColumnDefinition("nm_nome", VARCHAR),
                        new ApiColumnDefinition("nu_telefone", BIGINT),
                        new ApiColumnDefinition("ds_email", VARCHAR),
                        new ApiColumnDefinition("nu_cpf", VARCHAR))));
    }

    private static ApiTableDefinition createListaAtendimentoChat()
    {
        return new ApiTableDefinition(
                "lista_atendimento_chat",
                "lista-atendimento-chat",
                "dt_entrada",
                withRawJson(ImmutableList.of(
                        new ApiColumnDefinition("id_fila", BIGINT),
                        new ApiColumnDefinition("dt_entrada", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("id_campanha", BIGINT),
                        new ApiColumnDefinition("id_contato", BIGINT),
                        new ApiColumnDefinition("fl_status", BIGINT),
                        new ApiColumnDefinition("id_usuario", BIGINT),
                        new ApiColumnDefinition("dt_inicioatend", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("dt_finalatend", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("id_funil", BIGINT),
                        new ApiColumnDefinition("id_fase", BIGINT))));
    }

    private static ApiTableDefinition createListaMensagemAtendimentoChat()
    {
        return new ApiTableDefinition(
                "lista_mensagem_atendimento_chat",
                "lista-mensagem-atendimento-chat",
                "dt_mensagem",
                withRawJson(ImmutableList.of(
                        new ApiColumnDefinition("id_mensagens_atendimento", BIGINT),
                        new ApiColumnDefinition("dt_mensagem", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("id_fila", BIGINT),
                        new ApiColumnDefinition("ds_mensagem", VARCHAR),
                        new ApiColumnDefinition("fl_lida", BOOLEAN),
                        new ApiColumnDefinition("fl_direcao", BIGINT))));
    }

    private static ApiTableDefinition createListaAtendimentoChatbot()
    {
        return new ApiTableDefinition(
                "lista_atendimento_chatbot",
                "lista-atendimento-chatbot",
                "dt_criacao",
                withRawJson(ImmutableList.of(
                        new ApiColumnDefinition("id_contato", BIGINT),
                        new ApiColumnDefinition("dt_criacao", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("id_cliente", BIGINT),
                        new ApiColumnDefinition("fl_finalizado", BOOLEAN),
                        new ApiColumnDefinition("dt_finalizacao", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("id_funil", BIGINT),
                        new ApiColumnDefinition("id_lead", BIGINT),
                        new ApiColumnDefinition("id_departamento", BIGINT),
                        new ApiColumnDefinition("nm_nome", VARCHAR),
                        new ApiColumnDefinition("nu_telefone", VARCHAR),
                        new ApiColumnDefinition("ds_dados", VARCHAR),
                        new ApiColumnDefinition("ds_acesso", VARCHAR))));
    }

    private static ApiTableDefinition createListaMensagemAtendimentoChatbot()
    {
        return new ApiTableDefinition(
                "lista_mensagem_atendimento_chatbot",
                "lista-mensagem-atendimento-chatbot",
                "dt_criacao",
                withRawJson(ImmutableList.of(
                        new ApiColumnDefinition("id_contatomensagem", BIGINT),
                        new ApiColumnDefinition("dt_criacao", TIMESTAMP_MILLIS),
                        new ApiColumnDefinition("id_contato", BIGINT),
                        new ApiColumnDefinition("nu_ordem", BIGINT),
                        new ApiColumnDefinition("tp_mensagem", BIGINT),
                        new ApiColumnDefinition("tp_direcao", BIGINT),
                        new ApiColumnDefinition("nu_etapa", BIGINT),
                        new ApiColumnDefinition("ds_horario", VARCHAR),
                        new ApiColumnDefinition("ds_mensagem", VARCHAR))));
    }

    private static List<ApiColumnDefinition> withRawJson(List<ApiColumnDefinition> columns)
    {
        return ImmutableList.<ApiColumnDefinition>builder()
                .addAll(columns)
                .add(new ApiColumnDefinition("raw_json", VARCHAR))
                .build();
    }
}
