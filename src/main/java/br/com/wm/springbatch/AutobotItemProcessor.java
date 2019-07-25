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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

/**
 * Processador
 * Simples transformador que converte os nomes e carros para maiúsculo.
 * Objeto de entrada e saída.
 * Não é obrigatório que os objetos de entrada e saída sejam do mesmo tipo.
 */

public class AutobotItemProcessor implements ItemProcessor<Autobot, Autobot> {

    private static final Logger log = LoggerFactory.getLogger(AutobotItemProcessor.class);

    @Override
    public Autobot process(Autobot autobot) throws Exception {
        final String firstName = autobot.getName().toUpperCase();
        final String lastName = autobot.getCar().toUpperCase();

        final Autobot transformed = new Autobot(firstName, lastName);

        log.info("Converting (" + autobot + ") into (" + transformed + ")");

        return transformed;
    }
}