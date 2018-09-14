from sqlalchemy import create_engine
from sqlalchemy.engine import url
import pandas as pd
from django.http import HttpResponse
import cx_Oracle




def conectar(username, password, host, service_name, query, cpf):
    connect_url = url.URL(
        'oracle+cx_oracle',
        username=username,
        password=password,
        host=host,
        port='1521',
        query=dict(service_name=service_name))

    engine = create_engine(connect_url)
    df = pd.read_sql(query + cpf + "'", engine)

    return df


lista_username = ['luciaperon', 'luciaperon', 'gessicateodoro']

lista_password = ['LUCIAPERON', 'Mudar_123', 'A1b2c3d4$']

lista_host = ['krtn4-scan.pitagoras.apollo.br', 'krtn1-scan.pitagoras.apollo.br', 'krtn2-scan.pitagoras.apollo.br']

lista_bancos = ['bdprodexa', 'prod', 'AESA']

lista_query = ["""
            --- PRESENCIAL
            select distinct 
                   replace(replace(replace(replace(a.alucpf,'-',''),Chr(39),''),';',''),'.','') as CPF_ALUNO
                  ,replace(replace(replace(a.aluemail,'-',''),Chr(39),''),';','') as EMAIL_ALUNO
                  ,replace(replace(replace(a.alunome,'-',''),Chr(39),''),';','') as NOME_ALUNO
                  ,a.alucod as COD_ALUNO
                  ,'OLIM-'||b.camcod as COD_CAMPUS
                  ,b.camdesc as DESC_CAMPUS
                  ,e.espcod as COD_ESP
                  ,e.espdesc as DESC_ESP
                  ,dm.tmacod as COD_TMA
                  ,dm.discod as COD_DISC
                  ,d.disdesc as DESC_DISC
                  ,dm.dimetapa as ETAPA_DISC_GRADE
                  ,let.pelano||let.pelsem as ETAPA_DISCIPLINA_MINISTRADA
            ----------------------- Validações Filtros DNM
                  ,ce.desc_esp as FLAG_ELEGIVEL
            ----------------------- informações Studiare      
                  ,'O' as SISTEMA
                  ,hs.id_unidade_negocio as ID
                  ,hs.nome_unidade_negocio as UN_NEGOCIO
                  ,hs.id_regional as REGIONAL
                  ,hs.nome_campus as H_CAMPUS
                  ,hs.id_campus as ID_CAMPUS
                  ,ce.id_curso_studiare as CURSO_STUDIARE
                  ,vd.tipo_vinculo as TIPO
                  ,decode(asit.asisituacao,'C','Cursando'
                                          ,'D','Desistente'
                                          ,'E','Matrícula cancelada'
                                          ,'F','Formado'
                                          ,'N','Não rematriculado'
                                          ,'R','Transferido'
                                          ,'T','Matrícula trancada'
                                          ,'X','Expulso') as SIT_ALUNO
                   ,decode(ad.adisituacao,'A','Aprovado'
                                         ,'C','Cursando'
                                         ,'D','Desistente'
                                         ,'E','Necessita fazer exame'
                                         ,'F','Reprovado por falta'
                                         ,'M','Reprovado por média'
                                         ,'R','Reprovado por média e falta') as SIT_DISC                       


            from discmini dm
            ----------------------- JOINS Estruturais -----------------------------------
            join discipli d on d.discod = dm.discod
            join turma t on dm.tmacod = t.tmacod
            join especial e on e.espcod = t.espcod
                            and e.esptipo = 'G'
            join campus b on  b.camcod = e.espcampus
            join perileti let on let.pelcod = dm.pelcod
            join iuniempresa i on i.iunicodempresa = dm.iunicodempresa
            left join departam dep on dep.depcod = d.depcod                         
            join alundisc ad on ad.discod = dm.discod
                             and ad.tmacod = dm.tmacod
                             and ad.dimtip = dm.dimtip
            join alunos a on a.alucod = ad.alucod          
            left join v_alunsituatual asit on asit.alucod = ad.alucod
                                           and asit.espcod = ad.espcod
            -- valida alunos trocados
            left join especial ea on ea.espcod = ad.espcod
                                  and ea.esptipo = 'G'               

            ----------------------------- JOINS DNM -----------------------------------------                                                                                                    
                                  ---- HIERARQUIA Studiare                                                         
            left join robertoalmeida.dnm_tb_hierarquia_studiare hs on hs.cod_campus = 'OLIM-'||b.camcod
                                  ---- Filtro Cursos e Séries DNM
            left join robertoalmeida.dnm_tb_cursos_elegiveis ce on ce.desc_esp = e.espdesc
                                                 and ce.etapa_ministrada = let.pelano||let.pelsem
            left join victorrocha.tb_input_vinculo_disciplina vd on vd.disc_turma = dm.tmacod
                                                                 and vd.discod = to_char(dm.discod)                                                                                                                                          
            ----------------------------- FILTROS -----------------------------------------                         
            where let.pelano||let.pelsem = 20181 -- Valida Periodo Letivo
            and d.distipo = 'N' -- Só disciplinas Normais
            and e.espcod not in (116
                                ,3155
                                ,3702
                                ,13141
                                ,801
                                ,802) -- MED, FGV, etce
            and ce.desc_esp is not null -- Curso Elegível

            and a.alucpf = '""",



               """--- COLABORAR
                  SELECT distinct replace(replace(replace(replace(edaluno.ealu_nr_cpf,'-',''),Chr(39),''),';',''),'.','') as CPF
                  ,replace(replace(replace(edaluno.ealu_ds_mail,'-',''),Chr(39),''),';','') as EMAIL
                  ,replace(replace(replace(edaluno.ealu_nm,'-',''),Chr(39),''),';','') as NOME
                  ,edaluno.ealu_cd as RA
                  ,'C' as ORIGEM
                  ,case
                  when hierarquia.id_unidade_negocio is null then
                  'SEM HIERARQUIA'
                  else
                  hierarquia.id_unidade_negocio
                  end as UNIDADE_NEGOCIO
                  ,case
                  when hierarquia.id_regional is null then
                  'SEM HIERARQUIA'
                  else
                  hierarquia.id_regional
                  end as REGIONAL
                  ,case
                  when hierarquia.id_campus is null then
                  'SEM HIERARQUIA'
                  else
                  hierarquia.id_campus
                  end as CAMPUS
                  ,case
                  when cs.id_curso_studiare is null then
                  'SEM CURSO'
                  else
                  cs.id_curso_studiare
                  end as CURSO
                  ,edunidade.gepe_cd||'-'||edcurso.ecur_cd||'-'||edcurso.ecur_tp_turno||'-'||edmodulo.modu_ds_sigla as TURMA
                  ,'I' as ACAO -- pensar nas acoes
                  ,tb.grupo as GRUPO -- ACO, ALUNO, SEM NOTA
                  ,case
                  when tb.grupo = 'SEM NOTA' then 'Atividade Opcional'
                  else gediscipli.gdis_ds
                  end as NOME_DISCIPLINA
                  ,'VINCULOS_UNOPAR' as SEPARA
                  ,edcurso.ecur_cd as COD_CURSO
                  ,edcurso.ecur_nm as DESC_CURSO
                  ,gediscipli.gdis_cd as COD_DISC
                  ,edmodulo.modu_ds_sigla as ETAPA_DISC_GRADE
                  ,edmatric.emat_nr_ano || edmatric.emat_tp_semestre as ETAPA_DISC_MINISTRADA
                  ,tb.cod_curso as VALIDA_CURSO
                  ,hierarquia.id_regional as VALIDA_REGIONAL
                  ,cs.proj_cd as VALIDA_CURSO_STUDIARE
                  ,edcurhist.tpof_cd as TPOF_CD


                  from geprod.gecron gecron --Pasta
                  inner join geprod.gecronofer gecronofer
                  on gecronofer.gecr_cd = gecron.gecr_cd --Tabela Relativa com Oferta / Avaliação
                  inner join edprod.edcurhist edcurhist
                  on edcurhist.cuhi_id = gecronofer.cuhi_cd --Oferta*/
                  inner join geprod.geavaliac geavaliac
                  on geavaliac.gecr_cd = gecron.gecr_cd -- Data de Avaliação
                  inner join edprod.edmatric edmatric
                  on edmatric.cuhi_id = edcurhist.cuhi_id --Matricula
                  inner join edprod.edaluno edaluno
                  on edaluno.ealu_cd = edmatric.ealu_cd -- Aluno
                  left join edprod.edunidade edunidade
                  on edunidade.gepe_cd_polo = edmatric.gepe_cd_polo
                  and edunidade.unid_cd = edmatric.unid_cd --> Polo
                  inner join edprod.edmodulo edmodulo
                  on edmodulo.modu_cd = edcurhist.modu_cd -- Modulo
                  inner join edprod.edcurso edcurso
                  on edcurso.ecur_cd = edmodulo.ecur_cd --Curso
                  left join geprod.gecronlanc gecronlanc
                  on gecronlanc.gecr_cd = gecron.gecr_cd
                  and gecronlanc.gepe_cd = edaluno.gepe_cd --Lançamentos de avaliação
                  inner join geprod.gecrondisp gecrondisp
                  on gecrondisp.gcof_cd = gecronofer.gcof_cd
                  inner join geprod.GEDISCOFER GEDISCOFER
                  on GEDISCOFER.Gofd_Cd = gecrondisp.gofd_cd
                  inner join geprod.GEMATRDISC GEMATRDISC
                  on GEMATRDISC.gmdi_cd = GEDISCOFER.gmdi_cd
                  inner join geprod.gediscipli gediscipli
                  on gediscipli.gdis_cd = GEMATRDISC.Gdis_Cd
                  inner join edprod.ednotadisc ednotadisc
                  on ednotadisc.cuun_cd = edmatric.cuun_cd
                  inner join edprod.edprojeto edprojeto
                  on edprojeto.proj_cd = edcurhist.proj_cd
                  
                  -- Join com a tabela de Cursos participantes
                  
                  left join robertolemos.dnm_tb_cursos_participantes tb
                  on tb.cod_curso = edcurso.ecur_cd
                  and tb.etapa_disc_grade = edmodulo.modu_ds_sigla
                  and tb.proj_cd = edmatric.proj_cd
                  and tb.tpof_cd = edcurhist.tpof_cd
                  
                  -- Join com a tabela de hierarquia studiare
                  
                  left join robertolemos.dnm_tb_hierarquia_studiare hierarquia
                  on hierarquia.gepe_cd_polo = edunidade.gepe_cd_polo
                  
                  -- Cursos Participantes da Studiare
                  
                  left join robertolemos.dnm_tb_cursos_studiare cs
                  on cs.cod_curso = edcurso.ecur_cd
                  and cs.proj_cd = edmatric.proj_cd
                  
                  where edcurhist.cuhi_bo_cancelado = 'N'
                  and gecron.gecr_bo_cancelado = 'N'
                  and gecronofer.gcof_bo_cancelado = 'N'
                  -----------FILTROS---------------------------------------------------------------
                  and edmodulo.tpcs_cd = '03' -- 03-GRaduação EAD
                  and edmatric.sita_cd in ('01', '09', '27') --Situação do Aluno
                  and edcurhist.tpof_cd in ('1', '2') --Modalidade: (1-100%online | 2-Semipresencial)
                  and upper(gediscipli.gdis_ds) like '%SEMI%'
                  and edcurhist.cuhi_tp_oferta = 'R' --Oferta Regular
                  and ednotadisc.sitd_cd = 'DS' --Situação da Matricula (DS=Disciplina da Serie)      
                  --and edmatric.ealu_cd = 12892451 --Filtra Matricula
                  and edmatric.proj_cd = '32' -- Projeto(ano/Periodo)
                  ------------ FILTROS ALUNOS DNM -------------------------------------------------
                  and tb.cod_curso is not null
                  and hierarquia.id_regional is not null
                  and cs.proj_cd is not null
                  and edaluno.ealu_nr_cpf = '""",



               """----UNIDERP
                SELECT distinct replace(replace(replace(replace(pf.cpf,'-',''),Chr(39),''),';',''),'.','') as CPF_ALUNO
                  ,replace(replace(replace(ed.email,'-',''),Chr(39),''),';','') as EMAIL_ALUNO
                  ,replace(replace(replace(a.nome,'-',''),Chr(39),''),';','') as NOME_ALUNO
                  ,a.codigo as COD_ALUNO
                  ,'S' as SISTEMA
                  ,'EAD-UNIDERP' as UN_NEGOCIO
                  ,CASE WHEN A.UNIDADE = 'EAD' THEN to_char(up.idpolo)
                  ELSE A.UNIDADE
                  END as COD_CAMPUS
                  ,CASE WHEN A.UNIDADE = 'EAD' THEN up.nome
                  ELSE U.CURSONET
                  END  as DESC_CAMPUS
                  ,c.codigo as COD_CURSO
                  ,c.nome as DESC_CURSO
                  ,hs.codigo_polo_higienizado||'-'||c.codigo||'-'||c.turno||'-'||ca.serie ||'-'|| ca.turma as COD_TMA
                  ,ca.disciplina as COD_DISC
                  ,d.nome as DESC_DISC
                  ,ca.serie as ETAPA_DISC_GRADE
                  ,ca.Ano || ca.periodo as ETAPA_DISCIPLINA_MINISTRADA
                  ------------------- Selects do DNM -----------------------------------------------
                  ,hs.nome_unidade_negocio as NOME_UNIDADE_NEGOCIO
                  ,hs.id_unidade_negocio as ID_UNIDADE_NEGOCIO
                  ,hs.id_regional as ID_REGIONAL
                  ,hs.nome_campus as NOME_CAMPUS
                  ,hs.id_campus as ID_CAMPUS
                  ,eleg.id_curso_studiare as ID_CURSO_STUDIARE                                                                  
                  FROM
                  ----------------------------- JOINS ESTRUTURAIS DO BANCO ----------------------------------               
                  siae.unidade u INNER JOIN SIAE.ALUNO A ON A.UNIDADE = U.CODIGO
                  left join siae.pessoafisica pf on pf.unidade = a.unidade
                  and pf.codigo = a.codigo
                  left JOIN siae.endereco ed on ed.unidade = a.unidade
                  and ed.codigo = a.codigo
                  and ed.tipoentidade = 'ALU'
                  and ed.tipoendereco = 'RES'
                  
                  INNER JOIN SIAE.CURRICULOALUNO CA ON CA.UNIDADE = A.UNIDADE
                  AND CA.ALUNO = A.CODIGO
                  AND CA.ANO = extract(year from sysdate)
                  AND CA.PERIODO = case 
                  when extract(month from sysdate) >7 then 2
                  else 1 end
                  
                  
                  INNER JOIN SIAE.CURSO C ON C.UNIDADE = ca.UNIDADE
                  AND C.CODIGO = ca.CURSO
                  AND C.TIPO IN ('SER','CRE')
                  AND (C.UNIDADE_INTERATIVA = 'FIA')
                  
                  INNER JOIN SIAE.UNIDADE U ON U.CODIGO = A.UNIDADE
                  
                  LEFT JOIN SIAE.UNIDADE_POLO UP ON UP.IDPOLO = A.IDPOLO
                  AND UP.UNIDADE = A.UNIDADE
                  
                  INNER JOIN SIAE.DISCIPLINA D ON D.UNIDADE = CA.UNIDADE
                  AND D.CODIGO = CA.DISCIPLINA
                  
                  
                  INNER JOIN SIAE.DISCIPLINADETALHAMENTO DD ON DD.UNIDADE = CA.UNIDADE
                  AND DD.DISCIPLINA = CA.DISCIPLINA
                  AND DD.ANO = CA.ANO
                  AND DD.PERIODO = CA.PERIODO
                  ----------------------------- JOINS ESTRUTURAIS DO DNM ----------------------------------                                                                                              
                  inner join robertofilho.dnm_tb_curso_elegiveis eleg on eleg.cod_curso = ca.curso
                  and eleg.cod_disc = ca.disciplina
                  and eleg.etapa_disc_grade = ca.serie
                  and eleg.etapa_disciplina_ministrada = ca.ano||ca.periodo
                  left join robertofilho.dnm_tb_hierarquia_studiare hs on hs.unidade = a.unidade
                  and hs.id_polo = a.idpolo
                  where pf.cpf = '"""]
















cpf = 46490997819

for i in range(len(lista_bancos)):
    # transformar cpf in str
    cpf = str(cpf)

    username = lista_username[i]
    password = lista_password[i]
    host = lista_host[i]
    query = lista_query[i]

    # tratar cpf de acordo com o banco de dados
    service_name = lista_bancos[i]

    if service_name == 'bdprodexa':
        if len(cpf) == 11:
            # adicionar pontos e hifen
            cpf_aux = cpf[:3] + '.' + cpf[3:6] + '.' + cpf[6:9] + '-' + cpf[9:]
        elif len(str(cpf)) < 11:
            # transformar em str
            cpf_aux = str(cpf).zfill(11)

            # adicionar pontos e hifen
            cpf_aux = cpf[:3] + '.' + cpf[3:6] + '.' + cpf[6:9] + '-' + cpf[9:]
    else:
        cpf_aux = cpf.zfill(11)

    # função
    df = conectar(username, password, host, service_name, query, cpf_aux)

    if len(df.index) == 0:
        pass
    else:
        print(df)
        break





html = df.to_html()
