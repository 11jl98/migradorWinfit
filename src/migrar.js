const knex = require('./knex')
const fs = require('fs')
const { promisify } = require('util')
const path = require('path');
const moment = require('moment')

const access = promisify(fs.access);

async function Migrar() {
    const tables = await getListatables()
    console.log('> ' + tables.length)
  const dadosUpdate = []
  const dadosSave = []
  const dadosDelete = []
  
    for await (const item of tables) {
      try {
        const dadosUpdate = []
        const dadosSave = []
        const dadosDelete = []
        const nomeTabela = item.split('.')[0]
        const idTabela = item.split('.')[1]

        if(fs.existsSync(path.resolve('data/Save', `${nomeTabela}.json`))){
          console.log('>Save', nomeTabela)

          await access(path.resolve('data/Save', `${nomeTabela}.json`));
          dadosSave.push(await require(path.resolve('data/Save', `${nomeTabela}.json`)))
          await insertDados(dadosSave, nomeTabela, idTabela);
         }

        if(fs.existsSync(path.resolve('data/Update', `${nomeTabela}.json`))){
          console.log('>Update ' + nomeTabela)
          await access(path.resolve('data/Update', `${nomeTabela}.json`));
           dadosUpdate.push(await require(path.resolve('data/Update', `${nomeTabela}.json`)))
          await updateDados(dadosUpdate, nomeTabela, idTabela);

        }

        if(fs.existsSync(path.resolve('data/Delete', `${nomeTabela}.json`))){
          console.log('>Delete ' + nomeTabela)

          await access(path.resolve('data/Delete', `${nomeTabela}.json`));
          dadosDelete.push(await require(path.resolve('data/Delete', `${nomeTabela}.json`)))
          await deleteDados(dadosDelete, nomeTabela, idTabela);

        }
       
      } catch (error) {
        console.log("Error > ", error)
      }
    }
    console.log('finish')
  }

async function insertDados(data =[], tabela ="", idTabela =""){
    const dados = []

    for await ( const item of data){
      if (item.IDFAMILIAPROBLEMA == 0 || item.IDSOLO == 0 || item.IDORDEMPROBLEMA == 0) {
        ERROS_ID_ZERO.push(item);
        continue;
      }
  
      if (tabela === 'TIPOSPROBLEMAS' && item.IDTIPOPROBLEMA === 0) {
        ERROS_ID_ZERO.push(item);
        continue;
      }
  
      if (tabela === "CULTURAS") {
        if (!item.CODES)
          item.CODES = 0
  
        if (!item.CODMT)
          item.CODMT = 0
  
        if (!item.CODRS)
          item.CODRS = 0
  
        if (!item.CODSC)
          item.CODSC = 0
      }
  
      if (tabela === 'PROBLEMAS') {
        if (!item.CODSC)
          item.CODSC = 0
  
        if (!item.CODRS)
          item.CODRS = 0
  
        if (!item.CODMT)
          item.CODMT = 0
      }
  
      if (tabela === "FOTOSCULTURAS") {
        Reflect.deleteProperty(item, 'IDFOTO')
      }
  
      if (tabela === 'AGROTOXICOS') {
        item.CONDICAOCLIMATICA = item.CONDICAOCLIMATICAS
        item.MANEJOPRAGADOENCA = item.MANEJOPRAGASDOENCAS
        item.COD_SC = Number(item.COD_SC) === 0 ? null : item.COD_SC
        Reflect.deleteProperty(item, 'CONDICAOCLIMATICAS')
        Reflect.deleteProperty(item, 'MANEJOPRAGASDOENCAS')
      }
  
      if (tabela === 'EMBALAGENS') {
        if (!item.CAPACIDADE)
          item.CAPACIDADE = 0
        item.CAPACIDADE = parseFloat(item.CAPACIDADE)
      }
      Reflect.deleteProperty(item, idTabela)
      dados.push(item)
    }
    await knex.table(tabela.toLowerCase()).insert(dados)

}

async function updateDados(data =[], tabela = "", idTabela = ""){


    for await(const item of data){


      if (item.IDFAMILIAPROBLEMA == 0 || item.IDSOLO == 0 || item.IDORDEMPROBLEMA == 0) {
        ERROS_ID_ZERO.push(item);
        continue;
      }
  
      if (tabela === 'TIPOSPROBLEMAS' && item.IDTIPOPROBLEMA === 0) {
        ERROS_ID_ZERO.push(item);
        continue;
      }
  
      if (tabela === "CULTURAS") {
        if (!item.CODES)
          item.CODES = 0
  
        if (!item.CODMT)
          item.CODMT = 0
  
        if (!item.CODRS)
          item.CODRS = 0
  
        if (!item.CODSC)
          item.CODSC = 0
      }
  
      if (tabela === 'PROBLEMAS') {
        if (!item.CODSC)
          item.CODSC = 0
  
        if (!item.CODRS)
          item.CODRS = 0
  
        if (!item.CODMT)
          item.CODMT = 0
      }
  
      if (tabela === "FOTOSCULTURAS") {
        Reflect.deleteProperty(item, 'IDFOTO')
      }
  
      if (tabela === 'AGROTOXICOS') {
        item.CONDICAOCLIMATICA = item.CONDICAOCLIMATICAS
        item.MANEJOPRAGADOENCA = item.MANEJOPRAGASDOENCAS
        item.COD_SC = Number(item.COD_SC) === 0 ? null : item.COD_SC
        Reflect.deleteProperty(item, 'CONDICAOCLIMATICAS')
        Reflect.deleteProperty(item, 'MANEJOPRAGASDOENCAS')
      }
  
      if (tabela === 'EMBALAGENS') {
        if (!item.CAPACIDADE)
          item.CAPACIDADE = 0
        item.CAPACIDADE = parseFloat(item.CAPACIDADE)
      }
  const up = knex.table(tabela.toLowerCase()).update(item)

      if(tabela === "AGROTOXICOSCULTURAS"){
        up.where("IDAGROTOXICO", '=', item['IDAGROTOXICO'])
        .andWhere("IDCULTURA", '=', item["IDCULTURA"])
      }
      else if(tabela === "CLASSESAGROTOXICOS"){
        up.where("IDAGROTOXICO", '=', item["IDAGROTOXICO"])
        .andWhere("IDCLASSE", '=', item["IDCLASSE"])
      }
      else if(tabela === "CLASSESINGREDIENTESATIVOS"){
        up.where("IDCLASSE", '=', item["IDCLASSE"])
        .andWhere("IDINGREDIENTEATIVO", '=', item["IDINGREDIENTEATIVO"])
      }
      else if(tabela === "FOTOSCULTURAS"){
        up.where("IDFOTO", '=', item["IDFOTO"])
        .andWhere("IDCULTURA", '=', item["IDCULTURA"])
      }
      else if(tabela === "FOTOSINFESTACOES"){
        up.where("IDFOTO", '=', item["IDFOTO"])
        .andWhere("IDINFESTACAO", '=', item["IDINFESTACAO"])
      }
      else if(tabela === "FOTOSPROBLEMAS"){
        up.where("IDFOTO", '=', item["IDFOTO"])
        .andWhere("IDPROBLEMA", '=', item["IDPROBLEMA"])
      }
      else if(tabela === "RESTRICOESALVO"){
        up.where("IDAGROTOXICO", '=', item["IDAGROTOXICO"])
        .andWhere("IDCULTURA", '=', item["IDCULTURA"])
        .andWhere("IDPROBLEMA", '=', item["IDPROBLEMA"])
      }
      else if(tabela === "RESTRICOESCULTURAS"){
        up.where("IDAGROTOXICO", '=', item["IDAGROTOXICO"])
        .andWhere("IDCULTURA", '=', item["IDCULTURA"])
      }
      else{
        const value = item[idTabela]
        up.where(idTabela, '=', value)
      }
      console.log(tabela, idTabela)
      await up
    }

}

async function deleteDados(data =[], tabela = "",idTabela = ""){

  const del = knex.table(tabela.toLowerCase()).delete()
  for await ( const item of data){

 
    if(tabela === "AGROTOXICOSCULTURAS"){
      del.where("IDAGROTOXICO", '=', item[0])
      .andWhere("IDCULTURA", '=', item[1])
    }
    else if(tabela === "CLASSESAGROTOXICOS"){
      del.where("IDAGROTOXICO", '=', item[0])
      .andWhere("IDCLASSE", '=', item[1])
    }
    else if(tabela === "CLASSESINGREDIENTESATIVOS"){
      del.where("IDCLASSE", '=', item[0])
      .andWhere("IDINGREDIENTEATIVO", '=', item[1])
    }
    else if(tabela === "FOTOSCULTURAS"){
      del.where("IDFOTO", '=', item[0])
      .andWhere("IDCULTURA", '=', item[1])
    }
    else if(tabela === "FOTOSINFESTACOES"){
      del.where("IDFOTO", '=', item[0])
      .andWhere("IDINFESTACAO", '=', item[1])
    }
    else if(tabela === "FOTOSPROBLEMAS"){
      del.where("IDFOTO", '=', item[0])
      .andWhere("IDPROBLEMA", '=', item[1])
    }
    else if(tabela === "RESTRICOESALVO"){
      del.where("IDAGROTOXICO", '=', item[0])
      .andWhere("IDCULTURA", '=', item[1])
      .andWhere("IDPROBLEMA", '=', item[2])
    }
    else if(tabela === "RESTRICOESCULTURAS"){
      del.where("IDAGROTOXICO", '=', item[0])
      .andWhere("IDCULTURA", '=', item[1])
    }
    else{
      const value = item
      del.where(idTabela, '=', value)
    }



  }
  await del

}



async function getListatables() {
    return [
      "AGENTESBIOLOGICOS.IDAGENTEBIOLOGICO", "CATEGORIASPROBLEMAS.IDCATEGORIAPROBLEMA", "CLASSES.IDCLASSE", "CLASSESAMBIENTAIS.IDCLASSEAMBIENTAL",
      "CLASSESTOXICOLOGICAS.IDCLASSETOXICOLOGICA", "DISPOSICOESFINAIS.IDDISPOSICAOFINAL", "EMPRESAS.IDEMPRESA", "ESTADOSFISICOS.IDESTADOFISICO", "FAMILIASCULTURAS.IDFAMILIACULTURA", "FAMILIASPROBLEMAS.IDFOTOSPROBLEMAS",
      "FORMULACOES.IDFORMULACAO", "FOTOS.IDFOTO", "FUNCIONARIO.IDFUNCIONARIO", "GRAUSDEINFLAMABILIDADE.IDGRAUDEINFLAMABILIDADE", "GRUPOSQUIMICOS.IDGRUPOQUIMICO", "ORDENSPROBLEMAS.IDORDEMPROBLEMA", "PLANTASTOXICAS.IDPLANTATOXICA", "PRECAUCOES.IDPRECAUCAO", "PREVENCOES.IDPREVENCAO",
      "RELATATUALIZACAO.IDRELATATUALIZACAO", "SOLOS.IDSOLO", "SUBTIPOSPROBLEMAS.IDSUBTIPOPROBLEMA", "TIPOSAPLICACAO.IDTIPOAPLICACAO", "TIPOSEMBALAGENS.IDTIPOEMBALAGEM",
      "TIPOSPROBLEMAS.IDFOTOSPROBLEMAS", "UNIDADESDOSE.IDUNIDADEDOSE", "UNIDADESEMBALAGENS.IDUNIDADEEMBALAGEM", "UNIDADESVOLUMECALDA.IDUNIDADEVOLUMECALDA", "CULTURAS.IDCULTURA", "FOTOSCULTURAS.IDFOTOCULTURA",
      "PROBLEMAS.IDPROBLEMA", "INGREDIENTESATIVOS.IDINGREDIENTEATIVO", "CLASSESINGREDIENTESATIVOS.IDCLASSESINGREDIENTESATIVOS", "AGROTOXICOS.IDAGROTOXICO",
      "AGROTOXICOSCULTURAS.IDAGROTOXICOSCULTURAS", "APLICACOES.IDAPLICACAO",
      "CLASSESAGROTOXICOS.IDCLASSESAGROTOXICOS", "EMBALAGENS.IDEMBALAGEM",
      "FICHAEMERGENCIA.IDFICHAEMERGENCIA", "INFESTACOES.IDIFESTACAO", "FOTOSPROBLEMAS.IDFOTOSPROBLEMAS", 
      "INDICACOES.IDAPLICACAO", "RESTRICOESALVO.IDRESTRICOESALVO",
      "RESTRICOESCULTURAS.IDRESTRICOESCULTURAS", "FOTOSINFESTACOES.IDFOTOSINFESTACOES"
    ];
  }

Migrar()