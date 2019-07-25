/*
 * Copyright 2012-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package br.com.wm.springbatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

/**
 * Criando o processamento batch
 *
 */

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final DataSource dataSource;

    public BatchConfiguration(JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory,
                              DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    // tag::readerwriterprocessor[]
    /**
     * Entrada/Reader
     * Cria um ItemReader.
     * Ele procura por um arquivo chamado sample-data.csv e converte cada linha em um Autobot
     */
    @Bean
    public FlatFileItemReader<Autobot> reader() {
        FlatFileItemReader<Autobot> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("sample-data.csv"));
        reader.setLineMapper(new DefaultLineMapper<Autobot>() {
            {
            setLineTokenizer(new DelimitedLineTokenizer() {
                {
                setNames(new String[]{"name", "car"});
                }
            });
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Autobot>() {
                {
                setTargetType(Autobot.class);
                }
            });
            }
        });
        return reader;
    }

    /**
     * Processamento/Processor
     * Cria uma instancia do AutobotItemProcessor que foi definido anteriormente, para transformar os dados para maiúsculo.
     */
    @Bean
    public AutobotItemProcessor processor() {
        return new AutobotItemProcessor();
    }

    /**
     * Saída/Writer
     * Cria um ItemWriter
     * @return
     */
    @Bean
    public JdbcBatchItemWriter<Autobot> writer() {
        JdbcBatchItemWriter<Autobot> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setSql("INSERT INTO autobot (name, car) VALUES (:name, :car)");
        writer.setDataSource(this.dataSource);
        return writer;
    }
    // end::readerwriterprocessor[]


    // tag::jobstep[]
    /**
     * Define um processo (job)
     * Processos são construídos à partir de passos, onde cada passo envolve um reader, processor e um writer.
     * Na definição desse processo, precisa de um incrementer porque processos usam um banco de dados para manter o estado de execução.
     * Então lista cada passo, esse processo tem apenas um passo. O processo termina, e a API Java produz um processo perfeitamente configurado.
     */
    @Bean
    public Job importAutobotJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("importAutobotJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1())
                .end()
                .build();
    }

    /**
     * Define um passo (step)
     * Na definição do passo (step), é definido quantos dados quer escrever ao mesmo tempo.
     * Nesse caso, a aplicação escreve até 10 registros ao mesmo tempo (chunk).
     * Depois é configurado o reader, processor e writer injetando os métodos definidos anteriormente.
     */
    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<Autobot, Autobot>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }
    // end::jobstep[]
}
