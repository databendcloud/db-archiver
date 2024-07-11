import { DefaultFooter } from '@ant-design/pro-components';
import React from 'react';

const Footer: React.FC = () => {
  return (
    <DefaultFooter
      style={{
        background: 'none',
      }}
      links={[
        {
          key: 'Databend',
          title: `© ${new Date().getFullYear()} Databend Cloud. All Rights Reserved.`,
          href: 'https://www.databend.com/',
          blankTarget: true,
        },
      ]}
    />
  );
};

export default Footer;
