from django.shortcuts import render
from .forms import SignupForm

from sqlalchemy import create_engine
from sqlalchemy.engine import url
import pandas as pd
from django.http import HttpResponse
import cx_Oracle


# Create your views here.
def signupform(request):

    # if form is submitted
    if request.method == 'POST':
        # will handle the request later
        form = SignupForm(request.POST)
        # checking the form is valid or not
        if form.is_valid():

            def conectar(username, password, host, service_name, query, cpf, periodo):
                connect_url = url.URL(
                    'oracle+cx_oracle',
                    username=username,
                    password=password,
                    host=host,
                    port='1521',
                    query=dict(service_name=service_name))

                engine = create_engine(connect_url)
                df = pd.read_sql(query.format(cpf, periodo), engine)

                return df

            lista_username = ['luciaperon', 'luciaperon', 'gessicateodoro', 'flaviomoura']

            lista_password = ['LUCIAPERON', 'Mudar_123', 'A1b2c3d4$', 'kroton@2018']

            lista_host = ['krtn4-scan.pitagoras.apollo.br', 'krtn1-scan.pitagoras.apollo.br',
                          'krtn2-scan.pitagoras.apollo.br', '172.18.1.245']

            lista_bancos = ['bdprodexa', 'prod', 'AESA', 'bdfama']

            lista_query = ["""
                        --- PRESENCIAL
                        select distinct
                               replace(replace(replace(replace(a.alucpf,'-',''),Chr(39),''),';',''),'.','') as CPF
                              ,replace(replace(replace(a.aluemail,'-',''),Chr(39),''),';','') as EMAIL
                              ,replace(replace(replace(a.alunome,'-',''),Chr(39),''),';','') as NOME
                              ,a.alucod as RA
                              ,decode(asit.asisituacao,'C','Cursando'
                                                      ,'D','Desistente'
                                                      ,'E','Matrícula cancelada'
                                                      ,'F','Formado'
                                                      ,'N','Não rematriculado'
                                                      ,'P','Tombamento'
                                                      ,'R','Transferido'
                                                      ,'S','Transferido'
                                                      ,'T','Matrícula trancada'
                                                      ,'X','Expulso') as SIT_ALUNO
                               ,decode(ad.adisituacao,'A','Aprovado'
                                                     ,'C','Cursando'
                                                     ,'D','Desistente'
                                                     ,'E','Necessita fazer exame'
                                                     ,'F','Reprovado por falta'
                                                     ,'M','Reprovado por média'
                                                     ,'R','Reprovado por média e falta') as SIT_DISC
                              --,'OLIM-'||b.camcod as COD_CAMPUS
                              ,b.camdesc as DESC_CAMPUS
                              --,e.espcod as COD_ESP
                              ,dm.tmacod as TURMA
                              ,e.espdesc as DESC_CURSO
                               ,dm.dimetapa as SEMESTRE
                              ,CASE WHEN rd.discod = to_char(dm.discod) then rd.discod else to_char(dm.discod) END as COD_DISC
                              ,d.disdesc as DESC_DISC

                              ,let.pelano||let.pelsem as PERIODO
                        ----------------------- Validações Filtros DNM
                              --,ce.desc_esp as FLAG_ELEGIVEL
                        ----------------------- informações Studiare
                              ,'O' as SISTEMA
                              --,hs.id_unidade_negocio as ID
                              --,hs.nome_unidade_negocio as UN_NEGOCIO
                              --,hs.id_regional as REGIONAL
                              --,hs.nome_campus as H_CAMPUS
                              --,hs.id_campus as ID_CAMPUS
                              --,ce.id_curso_studiare as CURSO_STUDIARE
                              ,rd.vinculo as TIPO

                                                     ,T2.anonotaprova as NOTA_DNM
                                                    -- ,dm.pmecod as CODPARAMETRO
                                                    -- ,dm.usastudiare as VINCULO_STUDIARE


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
                        -- NOTA DNM
                        left join adisnota t2 on t2.alucod = ad.alucod
                        and t2.espcod = ad.espcod
                        and t2.discod = ad.discod
                        and t2.tmacod = ad.tmacod
                        and t2.dimtip = ad.dimtip
                        and t2.tprcod = 'T2'

                        ----------------------------- JOINS DNM -----------------------------------------
                                              ---- HIERARQUIA Studiare
                        left join robertoalmeida.dnm_tb_hierarquia_studiare hs on hs.cod_campus = 'OLIM-'||b.camcod
                                              ---- Filtro Cursos e Séries DNM
                        left join robertoalmeida.dnm_tb_cursos_elegiveis ce on ce.desc_esp = e.espdesc
                                                             and ce.etapa_ministrada = let.pelano||let.pelsem
                        left join robertoalmeida.dnm_tb_disc_elegiveis rd on rd.codtma = dm.tmacod
                        and rd.discod in (to_char(dm.discod), 'ACO')
                        and rd.vinculo = 'DNM'
                        ----------------------------- FILTROS -----------------------------------------
                        where d.distipo = 'N' -- Só disciplinas Normais
                        and e.espcod not in (116
                                            ,3155
                                            ,3702
                                            ,13141
                                            ,801
                                            ,802) -- MED, FGV, etce
                        -- and ce.desc_esp is not null -- Curso Elegível

                        and a.alucpf = '{}'
                        and let.pelano||let.pelsem = {} """,

                           """--- COLABORAR
                              SELECT distinct replace(replace(replace(replace(edaluno.ealu_nr_cpf,'-',''),Chr(39),''),';',''),'.','') as CPF
                              ,replace(replace(replace(edaluno.ealu_ds_mail,'-',''),Chr(39),''),';','') as EMAIL
                              ,replace(replace(replace(edaluno.ealu_nm,'-',''),Chr(39),''),';','') as NOME
                              ,edaluno.ealu_cd as RA
                              ,DECODE(edmatric.sita_cd,1,'Matricula Ativa'
                                                      ,9,'Formando'
                                                      ,27,'Matricula pré-confirmada') as SITUACAO
                             /* ,case
                              when hierarquia.id_unidade_negocio is null then
                              'SEM HIERARQUIA'
                              else
                              hierarquia.id_unidade_negocio
                              end as UNIDADE_NEGOCIO*/
                              /*,case
                              when hierarquia.id_regional is null then
                              'SEM HIERARQUIA'
                              else
                              hierarquia.id_regional
                              end as REGIONAL*/
                              ,case
                              when hierarquia.id_campus is null then
                              'SEM HIERARQUIA'
                              else
                              hierarquia.id_campus
                              end as CAMPUS
                              /*,case
                              when cs.id_curso_studiare is null then
                              'SEM CURSO'
                              else
                              cs.id_curso_studiare
                              end as CURSO*/
                              ,edunidade.gepe_cd||'-'||edcurso.ecur_cd||'-'||edcurso.ecur_tp_turno||'-'||edmodulo.modu_ds_sigla as TURMA
                              --,tb.grupo as GRUPO -- ACO, ALUNO, SEM NOTA
                              ,case
                              when tb.grupo = 'SEM NOTA' then 'Atividade Opcional'
                              else gediscipli.gdis_ds
                              end as NOME_DISCIPLINA
                              --,'VINCULOS_UNOPAR' as SEPARA
                              --,edcurso.ecur_cd as COD_CURSO
                              ,edcurso.ecur_nm as DESC_CURSO
                              ,edmodulo.modu_ds_sigla as SEMESTRE
                              ,gediscipli.gdis_cd as COD_DISC
                              ,edmatric.emat_nr_ano || edmatric.emat_tp_semestre as PERIODO
                              ,'C' as SISTEMA
                              --,tb.cod_curso as VALIDA_CURSO
                              --,hierarquia.id_regional as VALIDA_REGIONAL
                              --,cs.proj_cd as VALIDA_CURSO_STUDIARE
                              --,edcurhist.tpof_cd as TPOF_CD
                              ,CASE WHEN tb.cod_curso = edcurso.ecur_cd and tb.etapa_disc_grade = edmodulo.modu_ds_sigla
                              then 'DNM' else 'NONE' end as TIPO_VINCULO
                              --,ednotadisc.sitd_cd as sit_aluno
                              --,gecron.gecr_cd as COD_ATIVIDADE
                              --,gecron.gecr_ds as DESC_ATIVIDADE
                              --,gecron.gect_cd as TIPO_ATIVIDADE
                              --,gecronofer.gcof_dt_fim as FIM_ATIVIDADE
                              ,gecronlanc.gecv_cd as CONCEITO
                              ,decode(gecronlanc.gecv_cd,1,'EXCELENTE'
                                                        ,2,'MUITO BOM'
                                                        ,3,'BOM'
                                                        ,4,'SUFICIENTE'
                                                        ,5,'INSUFICIENTE'
                                                        ,10,'SEM CONCEITO') as DESC_CONCEITO


                              from geprod.gecron gecron --Pasta
                              inner join geprod.gecronofer gecronofer
                              on gecronofer.gecr_cd = gecron.gecr_cd --Tabela Relativa com Oferta / Avaliação
                              inner join edprod.edcurhist edcurhist
                              on edcurhist.cuhi_id = gecronofer.cuhi_cd --Oferta
                              /*inner join geprod.geavaliac geavaliac
                              on geavaliac.gecr_cd = gecron.gecr_cd -- Data de Avaliação*/
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
                              and gecronlanc.gcof_cd = gecronofer.gcof_cd -- Oferta de cronograma vi
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
                              --and edmatric.proj_cd = '32' -- Projeto(ano/Periodo)
                  ------------ FILTROS ALUNOS DNM -------------------------------------------------
                              --and tb.cod_curso is not null
                              --and hierarquia.id_regional is not null
                              --and cs.proj_cd is not null
                              -------------- FILTROS TIME ------------------------------------------------------
                              --and gecron.gect_cd in (1,3) -- 1- Portfolio, 3 - Leitura (1 e 3 DNM), 22- Avaliacao Proficiencia
                              --and gecron.gecr_bo_media = 'S'
                              and edaluno.ealu_nr_cpf = '{}'
                              and edmatric.emat_nr_ano || edmatric.emat_tp_semestre = {} """,

                           """----UNIDERP
                                           SELECT distinct replace(replace(replace(replace(pf.cpf,'-',''),Chr(39),''),';',''),'.','') as CPF
                                             ,replace(replace(replace(ed.email,'-',''),Chr(39),''),';','') as EMAIL
                                             ,replace(replace(replace(a.nome,'-',''),Chr(39),''),';','') as NOME
                                             ,a.codigo as RA
                                             ,a.situacao as SITUACAO
                                             --,'EAD-UNIDERP' as UN_NEGOCIO
                                             ,CASE WHEN A.UNIDADE = 'EAD' THEN to_char(up.idpolo)
                                             ELSE A.UNIDADE
                                             END as COD_CAMPUS
                                             ,CASE WHEN A.UNIDADE = 'EAD' THEN up.nome
                                             ELSE U.CURSONET
                                             END  as DESC_CAMPUS
                                             --,c.codigo as COD_CURSO
                                             ,hs.codigo_polo_higienizado||'-'||c.codigo||'-'||c.turno||'-'||ca.serie ||'-'|| ca.turma as TURMA
                                             ,c.nome as DESC_CURSO
                                              ,ca.serie as SEMESTRE
                                              ,d.nome as DESC_DISC
                                              ,ca.disciplina as COD_DISC
                                             ,ca.Ano || ca.periodo as PERIODO
                                             --,'S' as SISTEMA
                                             ,case when eleg.etapa_disciplina_ministrada = ca.ano||ca.periodo then 'DNM'
                                             else 'NONE' end as TIPO
                                             ,mn.nota as NOTA

                                             ------------------- Selects do DNM -----------------------------------------------
                                             --,hs.nome_unidade_negocio as NOME_UNIDADE_NEGOCIO
                                             --,hs.id_unidade_negocio as ID_UNIDADE_NEGOCIO
                                             --,hs.id_regional as ID_REGIONAL
                                             --,hs.nome_campus as NOME_CAMPUS
                                             --,hs.id_campus as ID_CAMPUS
                                             --,eleg.id_curso_studiare as ID_CURSO_STUDIARE
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
                                             --AND CA.ANO = extract(year from sysdate)
                                             --AND CA.PERIODO = case
                                             --when extract(month from sysdate) >7 then 2
                                             --else 1 end


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

                                             left join siae.moodle_nota mn on mn.aluno = a.codigo
                                             and mn.disciplina = ca.disciplina

                                             ----------------------------- JOINS ESTRUTURAIS DO DNM ----------------------------------
                                             left join robertofilho.dnm_tb_curso_elegiveis eleg on eleg.cod_curso = ca.curso
                                             and eleg.cod_disc = ca.disciplina
                                             and eleg.etapa_disc_grade = ca.serie
                                             and eleg.etapa_disciplina_ministrada = ca.ano||ca.periodo
                                             left join robertofilho.dnm_tb_hierarquia_studiare hs on hs.unidade = a.unidade
                                             and hs.id_polo = a.idpolo
                                             where pf.cpf = '{}'
                                             and ca.ano||ca.periodo = {} """,
                           """--- FAMA
                           select distinct
                           replace(replace(replace(replace(a.alucpf,'-',''),Chr(39),''),';',''),'.','') as CPF
                           ,replace(replace(replace(a.aluemail,'-',''),Chr(39),''),';','') as EMAIL
                           ,replace(replace(replace(a.alunome,'-',''),Chr(39),''),';','') as NOME
                           ,a.alucod as RA
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
                           ,b.camdesc as DESC_CAMPUS
                           --,e.curcod as COD_ESP
                           --,ea.espcod as COD_ESP_ALUNO
                           ,dm.tmacod as COD_TMA
                           ,e.espdesc as DESC_ESP
                           ,dm.dimetapa as SEMESTRE
                           ,dm.discod as COD_DISC
                           ,d.disdesc as DESC_DISC
                           ,let.pelano||let.pelsem as PERIODO
                  ----------------------- SITUAÇÃO DO ALUNO

                  ----------------------- informações Studiare
                  ,'O' as SISTEMA
                  ,'DNM' as TIPO
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

                    -- valida alunos trocados
                    left join especial ea on ea.espcod = ad.espcod
                    and ea.esptipo = 'G'
                    --VALIDA SITUACAO ALUNO
                    join alundisc ad on ad.discod = dm.discod
                    and ad.tmacod = dm.tmacod
                    and ad.dimtip = dm.dimtip
                    join alunos a on a.alucod = ad.alucod
                    left join v_alunsituatual asit on asit.alucod = ad.alucod
                    and asit.espcod = ad.espcod
                    where  dm.tmacod||-dm.discod in
                    ('10120151A-100399',
                        '11220141A-10104',
                        '11220142A-10104',
                        '11420141A-10104',
                        '11420142A-10315',
                        '1520151A-64387',
                        '1520152A-64387',
                        '1620151A-64387',
                        '1620152A-64387',
                        '1620161A-64387',
                        '1620162A-64387',
                        '20920151A-9177',
                        '20920152A-9177',
                        '21020151A-9177',
                        '21020152A-9177',
                        '21720141A-7943',
                        '21820141A-9430',
                        '21820142A-9430',
                        '2520142A-100377',
                        '2520151A-100377',
                        '2620151A-64482',
                        '4420172A-64435',
                        '4820161A-88408',
                        '4820162A-88408',
                        '5020171A-10217',
                        '5020172A-10217',
                        '40520151A-9084')
                        and a.alucpf = '{}'
                        and let.pelano||let.pelsem = {} """
                           ]

            cpf = form.cleaned_data['cpf']
            periodo = form.cleaned_data['periodo']

            for i in range(len(lista_bancos)):
                # transformar cpf in str
                cpf = str(cpf)

                username = lista_username[i]
                password = lista_password[i]
                host = lista_host[i]
                query = lista_query[i]

                # tratar cpf de acordo com o banco de dados
                service_name = lista_bancos[i]

                if service_name in ['bdprodexa', 'bdfama']:
                    cpf_aux = cpf.replace('.', '')
                    cpf_aux = cpf_aux.replace('-', '')
                    if len(cpf_aux) == 11:
                        # adicionar pontos e hifen
                        cpf_aux = cpf_aux[:3] + '.' + cpf_aux[3:6] + '.' + cpf_aux[6:9] + '-' + cpf_aux[9:]
                    elif len(str(cpf_aux)) < 11:
                        # transformar em str
                        cpf_aux = str(cpf_aux).zfill(11)

                        # adicionar pontos e hifen
                        cpf_aux = cpf_aux[:3] + '.' + cpf_aux[3:6] + '.' + cpf_aux[6:9] + '-' + cpf_aux[9:]
                else:
                    cpf_aux = cpf.replace('.', '')
                    cpf_aux = cpf_aux.replace('-', '')
                    cpf_aux = cpf_aux.zfill(11)

                # função
                df = conectar(username, password, host, service_name, query, cpf_aux, periodo)

                if len(df.index) == 0:
                    pass

                else:
                    break

            html = df.to_html()
            return HttpResponse(html)


    else:
        # creating a new form
        form = SignupForm()

        # returning form

        return render(request, 'signupform.html', {'form': form});
