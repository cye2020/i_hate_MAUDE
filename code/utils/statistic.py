# =========================================================
# 통계 검정 클래스
# ========================================================="


# 1. 표준 라이브러리
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

# 2. 서드파티 라이브러리
import pandas as pd
import numpy as np
from numpy.typing import ArrayLike
from IPython import get_ipython
from IPython.display import display

# 2-1. 시각화 라이브러이
import matplotlib.pyplot as plt

# 2-2. 통계 분석
from scipy import stats
from scipy.stats import shapiro, levene, ttest_ind, chi2_contingency
from scipy.stats import mannwhitneyu


def is_running_in_notebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter Notebook or JupyterLab
        elif shell == 'TerminalInteractiveShell':
            return False  # 일반 IPython 터미널
        else:
            return False
    except (NameError, ImportError):
        return False  # Pure Python 환경

display = display if is_running_in_notebook() else print


@dataclass
class TestResult:
    test_name: str
    statistic: float
    p_value: float
    effect_size: float
    effect_interpretation: str
    conclusion: str
    metadata: dict = None

    def is_significant(self, alpha: float = 0.05) -> bool:
        return self.p_value < alpha
    
    def to_dict(self) -> dict:
        return self.__dict__


class StatisticalTest(ABC):
    """
    모델 통계 검정의 기반 추상 클래스
    """
    
    @abstractmethod
    def execute(self):
        pass
    
    @abstractmethod
    def interpret(self):
        pass


class TTest(StatisticalTest):
    """
    t-검정 (독립/대응)
    """
    
    def __init__(self):
        self.test_name = 't-검정'
    
    def execute_all(self, data: pd.DataFrame, iv_col: str, dv_cols: List[str], labels: list = [0, 1], alpha: float = 0.05):
        result_lst =[]
        for dv_col in dv_cols:
            result = self.execute(data, iv_col=iv_col, dv_col=dv_col, labels=labels, alpha=alpha)
            result_lst.append(result)
        return result_lst
    
    def execute(self, data: pd.DataFrame, iv_col: str, dv_col: str, labels: list = [0, 1], alpha: float = 0.05):
        """
        두 그룹 간 평균 차이에 대한 가설검정을 수행하는 함수.
        (정규성에 따라 t-검정 또는 Mann-Whitney U 검정을 자동 선택)

        Parameters
        ----------
        data: pd.DataFrame
            원본 데이터
        iv_col: str
            독립 변수 컬럼명
        dv_col: str
            종속 변수 컬럼명
        labels: list
            독립 변수의 값들
        alpha : float, optional
            유의수준 (default=0.05)

        Returns
        -------
        result : TestResult
        """
        class0_data = data[data[iv_col] == labels[0]][dv_col].dropna()
        class1_data = data[data[iv_col] == labels[1]][dv_col].dropna()
        
        self.plot(class0_data, class1_data, labels, iv_col, dv_col)
        
        stat, p_levene, equal_var = self.check_homosecedasticity(class0_data, class1_data)
        
        is_normal_0 = self.check_normality_simple(class0_data)
        is_normal_1 = self.check_normality_simple(class1_data)
        
        print("\n[가설검정]")
        print("-" * 40)

        if is_normal_0 and is_normal_1:
            print("H₀: μ₀ = μ₁ (두 클래스의 평균이 같다)")
            print("H₁: μ₀ ≠ μ₁ (두 클래스의 평균이 다르다)")
        else:
            print("H₀: 두 클래스의 분포가 같다 (중앙값 차이가 없다)")
            print("H₁: 두 클래스의 분포가 다르다 (중앙값 차이가 있다)")
            
        print(f"유의수준: α = {alpha}\n")

        # --- 검정 수행 ---
        if is_normal_0 and is_normal_1:
            # 모수 검정
            test_name = "Student's t-test" if equal_var else "Welch's t-test"
            t_stat, p_value = ttest_ind(class0_data, class1_data, equal_var=equal_var)
            print(f"{test_name} 결과:")
            print(f"t = {t_stat:.4f}, p = {p_value:.4f}")
            
            # Cohen's d 계산
            pooled_std = np.sqrt((class0_data.var() + class1_data.var()) / 2)
            cohens_d = (class0_data.mean() - class1_data.mean()) / pooled_std
            abs_d = abs(cohens_d)

            if abs_d < 0.2:
                effect = "매우 작은 효과"
            elif abs_d < 0.5:
                effect = "작은 효과"
            elif abs_d < 0.8:
                effect = "중간 효과"
            else:
                effect = "큰 효과"

            print(f"Cohen's d = {cohens_d:.3f} ({effect})")

            test_stat = t_stat
            effect_size = cohens_d
            effect_interpretation = effect

        else:
            # 비모수 검정
            test_name = "Mann-Whitney U test"
            u_stat, p_value = mannwhitneyu(class0_data, class1_data, alternative='two-sided')
            print(f"{test_name} 결과:")
            print(f"U = {u_stat:.4f}, p = {p_value:.4f}")
            
            # 총 샘플 크기 (N)
            n0 = len(class0_data)
            n1 = len(class1_data)
            N = n0 + n1
            
            # 효과 크기 계산 (rank-biserial crrelation)
            r_rb = (2 * u_stat) / (n0 * n1) - 1
            abs_rb = abs(r_rb)
            
            if abs_rb < 0.1:
                effect = "매우 작은 효과"
            elif abs_rb < 0.3:
                effect = "작은 효과"
            elif abs_rb < 0.5:
                effect = "중간 효과"
            else:
                effect = "큰 효과"
            
            test_stat = u_stat
            effect_size = r_rb
            effect_interpretation = effect

        # --- 결론 ---
        print("\n[결론]")
        if p_value < alpha:
            conclusion = f"✅ p-value({p_value:.4f}) < {alpha} → 귀무가설 기각\n   두 클래스에 유의한 차이가 있음"
        else:
            conclusion = f"❌ p-value({p_value:.4f}) ≥ {alpha} → 귀무가설 채택\n   두 클래스에 유의한 차이가 없음"

        print(conclusion)
        
        metadata = {
            'f_stat': stat,
            'p_levene': p_levene
        }
        
        # 결과 반환
        return TestResult(
            test_name=test_name,
            statistic=test_stat,
            p_value=p_value,
            effect_size=effect_size,
            effect_interpretation=effect_interpretation,
            conclusion=conclusion,
            metadata=metadata
        )
    
    def interpret(self, result: TestResult, alpha: float = 0.05) -> None:
        if result.p_value >= alpha:
            pass

    @staticmethod
    def check_normality_simple(data: ArrayLike, col="데이터"):
        """
        데이터의 정규성을 검정하는 함수: t-test용

        Parameters
        ----------
        data : array-like
            정규성을 검정할 데이터 (NaN은 자동 제거)
        name : str, default="데이터"
            출력 시 표시될 데이터 이름

        Returns
        -------
        bool
            정규분포 가정 충족 여부
            - True: 정규분포 가정 가능 (모수 검정)
            - False: 정규분포 가정 위반 (비모수 검정)

        검정 기준
        ---------
        - n < 30: Shapiro-Wilk 검정 (p > 0.05)
        - 30 ≤ n < 100: 왜도/첨도 우선, 필요시 Shapiro-Wilk
        - n ≥ 100: 왜도 기준 (|왜도| < 2, 중심극한정리)
        """
        # NaN 체크
        if pd.isna(data).any():
            print(f"⚠️ 경고: {col}에 NaN 값이 {pd.isna(data).sum()}개 포함됨")
            data = data.dropna()
            print(f"   → NaN 제거 후 n={len(data)}")

        n = len(data)

        print(f"\n[{col} 정규성 검정] n={n}")
        print("-"*40)

        # 왜도와 첨도
        skew = stats.skew(data)
        kurt = stats.kurtosis(data, fisher=True)
        print(f"왜도(Skewness): {skew:.3f}")
        print(f"첨도(Kurtosis): {kurt:.3f}")

        # 표본 크기에 따른 판단
        if n < 30:
            stat, p = shapiro(data)
            print(f"Shapiro-Wilk p-value: {p:.4f}")
            is_normal = p > 0.05
            reason = f"Shapiro p={'>' if is_normal else '≤'}0.05"
        elif n < 100:
            if abs(skew) < 1 and abs(kurt) < 2:
                is_normal = True
                reason = "|왜도|<1, |첨도|<2"
            else:
                stat, p = shapiro(data)
                print(f"추가 Shapiro-Wilk p-value: {p:.4f}")
                is_normal = p > 0.05
                reason = f"Shapiro p={'>' if is_normal else '≤'}0.05"
        else:
            is_normal = abs(skew) < 2
            reason = f"|왜도|{'<' if is_normal else '≥'}2 (중심극한정리)"

        print(f"결과: {'✅ 정규분포 가정 충족' if is_normal else '❌ 정규분포 가정 위반'} ({reason})")
        return is_normal
    
    @staticmethod
    def check_homosecedasticity(class0_data, class1_data):
        print("\n[등분산성 검정]")
        print("-"*40)
        stat, p_levene = levene(class0_data, class1_data)
        print(p_levene)
        print(f"Levene's test p-value: {p_levene:.4f}")
        equal_var = p_levene > 0.05
        return stat, p_levene, equal_var
    
    @staticmethod
    def plot(class0_data, class1_data, labels, iv_col, dv_col):
        fig, axes = plt.subplots(1, 3, figsize=(14, 5))

        # 박스플롯
        bp = axes[0].boxplot([class0_data, class1_data],
                            labels=labels,
                            patch_artist=True)
        bp['boxes'][0].set_facecolor('lightblue')
        bp['boxes'][1].set_facecolor('lightcoral')
        axes[0].set_ylabel(dv_col)
        axes[0].set_title(f'{dv_col} 분포')
        axes[0].grid(True, alpha=0.3)

        # 히스토그램
        axes[1].hist(class0_data, bins=10, alpha=0.6, label=f'{iv_col} - {labels[0]}', 
                    color='blue', density=True, edgecolor='black')
        axes[1].hist(class1_data, bins=10, alpha=0.6, label=f'{iv_col} - {labels[1]}', 
                    color='red', density=True, edgecolor='black')
        axes[1].set_xlabel(dv_col)
        axes[1].set_ylabel('밀도')
        axes[1].set_title(f'{dv_col} 분포 비교')
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)

        # Q-Q plot (Class 0)
        stats.probplot(class0_data, dist="norm", plot=axes[2])
        axes[2].set_title(f'Q-Q Plot ({labels[0]})')
        axes[2].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.show()


class Chi2Test(StatisticalTest):
    def __init__(self):
        super().__init__()
        self.test_name = '카이제곱 검정'
    
    def execute_all(self, data: pd.DataFrame, iv_col: str, dv_cols: List[str], alpha: float = 0.05):
        result_lst =[]
        for dv_col in dv_cols:
            result = self.execute(data, iv_col, dv_col, alpha)
            result_lst.append(result)
        return result_lst
    
    def execute(self, data: pd.DataFrame, iv_col: str, dv_col: str, alpha: float = 0.05):
        df = data[[iv_col, dv_col]]
        
        # 교차표
        contingency_table = pd.crosstab(df[iv_col], df[dv_col])
        
        # 기대빈도 확인
        is_valid = self.check_expected_frequencies(contingency_table)
        
        # 카이제곱 검정
        chi2_stat, p_value, dof, expected = chi2_contingency(contingency_table)
        
        n = contingency_table.values.sum()  # 전체 표본 수
        r, c = contingency_table.shape      # 행수, 열수
        
        test_stat = chi2_stat
        # ==============================================
        # Cramér's V 값 (0~1 사이)
        # - 0에 가까울수록: 독립적 (연관성 없음)
        # - 1에 가까울수록: 강한 연관성
        # ==============================================
        v = np.sqrt(chi2_stat / (n * min(r-1, c-1)))
        
        # Cramér's V 값 효과 크기 해석
        if v < 0.1:
            effect = "매우 약한 관계"
        elif v < 0.3:
            effect = "약한 관계"
        elif v < 0.5:
            effect = "중간 관계"
        else:
            effect = "강한 관계"
        
        effect_size = v
        effect_interpretation = effect
        
        # 기대빈도 테이블
        expected_df = pd.DataFrame(
            expected, 
            index=contingency_table.index,
            columns=contingency_table.columns
        )
        print("\n[기대빈도]")
        print(expected_df.round(2))

        
        # ==============================================
        # 표준화 잔차
        # - |잔차| > 2: 해당 셀이 독립성에서 유의하게 벗어남
        # - |잔차| > 3: 매우 강한 연관성 (이상치 수준)
        # - 양수: 관측값이 기대값보다 큼 (과대 표현)
        # - 음수: 관측값이 기대값보다 작음 (과소 표현)
        # ==============================================
        std_residuals = (contingency_table.values - expected) / np.sqrt(expected)
        residuals_df = pd.DataFrame(
            std_residuals,
            index=contingency_table.index,
            columns=contingency_table.columns
        )
        print("\n[표준화 잔차]")
        display(residuals_df.round(2))
        print("(|잔차| > 2: 유의한 차이, |잔차| > 3: 매우 강한 연관성)")
        
        # 결과 요약
        print("\n[결론]")
        if p_value < alpha:
            conclusion = f"✅ p-value({p_value:.4f}) < {alpha} → 귀무가설 기각\n" \
                            f"   {iv_col}과(와) {dv_col}은(는) 관련이 있음" \
                            f"   효과 크기: {effect_interpretation}"
            print(conclusion)
            
            # 사후분석
            print("\n[사후분석]")
            print("   표준화 잔차 |값| > 2인 셀 해석:")
            post_hoc = "   표준화 잔차 |값| > 2인 셀 해석:"
            for i, row_label in enumerate(contingency_table.index):
                for j, col_label in enumerate(contingency_table.columns):
                    if abs(std_residuals[i, j]) > 2:
                        if std_residuals[i, j] > 0:
                            post_hoc += f"\n   • {row_label} - {col_label}: 예상보다 많음 (잔차={std_residuals[i, j]:.2f})"
                        else:
                            post_hoc += f"\n   • {row_label} - {col_label}: 예상보다 적음 (잔차={std_residuals[i, j]:.2f})"
            print(post_hoc)
            metadata = {'post_hoc': post_hoc}
        else:
            conclusion = f"❌ p-value({p_value:.4f}) ≥ {alpha} → 귀무가설 채택" \
                            f"   {iv_col}과(와) {dv_col}은(는) 독립적임 (연관 없음)"
            print(conclusion)
            metadata = {}
        
        return TestResult(
            test_name=self.test_name,
            statistic=test_stat,
            p_value=p_value,
            effect_size=effect_size,
            effect_interpretation=effect_interpretation,
            conclusion=conclusion,
            metadata=metadata
        )

    
    def interpret(self):
        return super().interpret()

    @staticmethod
    def cramers_v(chi2_stat, n, r, c):
        """
        Cramér's V 효과 크기 계산
        
        카이제곱 검정의 효과 크기를 측정하는 지표로, 두 범주형 변수 간
        연관성의 강도를 0~1 사이 값으로 표현합니다.
        
        Parameters
        ----------
        chi2_stat : float
            카이제곱 통계량 (χ²)
        n : int
            전체 표본 수 (분할표의 총합)
        r : int
            행(row)의 개수
        c : int
            열(column)의 개수
        
        Returns
        -------
        float
            Cramér's V 값 (0~1 사이)
            - 0에 가까울수록: 독립적 (연관성 없음)
            - 1에 가까울수록: 강한 연관성
        """
        return np.sqrt(chi2_stat / (n * min(r-1, c-1)))
    
    @staticmethod
    def check_expected_frequencies(contingency_table):
        """
        카이제곱 검정의 기대빈도 가정 확인
        
        카이제곱 검정을 수행하기 전에 기대빈도가 충분한지 검사합니다.
        기대빈도가 너무 작으면 카이제곱 검정의 정확도가 떨어집니다.
        
        Parameters
        ----------
        contingency_table : array-like
            분할표 (관측 빈도)
        
        Returns
        -------
        bool
            카이제곱 검정 사용 가능 여부
            - True: 카이제곱 검정 사용 가능
            - False: Fisher's exact test 권장
        
        검정 기준
        ---------
        1. 모든 기대빈도 ≥ 5 (이상적)
        2. 기대빈도 < 5인 셀이 전체의 20% 이하 (허용 가능)
        
        Notes
        -----
        - 2×2 분할표에서 기대빈도 < 5인 경우: Fisher's exact test 필수
        - 큰 분할표에서 일부 셀만 < 5: 카이제곱 검정 여전히 사용 가능
        """
        # 카이제곱 검정으로 기대빈도 계산
        chi2_stat, p_val, dof, expected = chi2_contingency(contingency_table)
        
        print("\n[기대빈도 확인]")
        print("-"*40)
        
        # -------------------------------------------------------------------------
        # 1. 최소 기대빈도 확인
        # -------------------------------------------------------------------------
        min_expected = expected.min()
        print(f"최소 기대빈도: {min_expected:.2f}")
        
        # -------------------------------------------------------------------------
        # 2. 기대빈도 < 5인 셀의 비율 계산
        # -------------------------------------------------------------------------
        cells_below_5 = (expected < 5).sum()  # 5 미만인 셀 개수
        total_cells = expected.size  # 전체 셀 개수
        percent_below_5 = (cells_below_5 / total_cells) * 100
        
        print(f"5 미만 셀: {cells_below_5}/{total_cells} ({percent_below_5:.1f}%)")
        
        # -------------------------------------------------------------------------
        # 3. 카이제곱 검정 적합성 판단
        # -------------------------------------------------------------------------
        # 조건: 최소 기대빈도 ≥ 5 AND 5 미만 셀 비율 ≤ 20%
        if min_expected < 5 or percent_below_5 > 20:
            print("⚠️ 주의: Fisher's exact test 사용 권장")
            print("   (기대빈도가 너무 작아 카이제곱 검정 부정확)")
            return False
        else:
            print("✅ 카이제곱검정 사용 가능")
            return True